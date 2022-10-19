using KeyMapSync.Entity;
using SqModel;
using SqModel.Analysis;
using SqModel.Dapper;
using SqModel.Expression;
using System.Data;

namespace KeyMapSync;

public class OffsetSynchronizer
{
    public OffsetSynchronizer(IDbConnection connection, Datasource datasource, IDBMS dbms, Func<string, Destination> resolver, Action<SelectQuery>? injector = null)
    {
        if (datasource == null) throw new ArgumentNullException();

        Connection = connection;
        Datasource = datasource;
        Injector = injector;
        Dbms = dbms;
        DestinationResolver = resolver;

        Datasource.Destination = DestinationResolver(Datasource.DestinationName);

        var keymap = datasource.Destination?.KeyMapConfig;
        var versioning = datasource.Destination?.VersioningConfig;
        var offset = datasource.Destination?.OffsetConfig;

        if (keymap == null) throw new InvalidOperationException();
        if (offset == null) throw new InvalidOperationException();
        if (versioning == null) throw new InvalidOperationException();

        VersioningConfig = versioning;
        VersionConfig = VersioningConfig.VersionConfig;
        OffsetConfig = offset;

        var tmp = BridgeNameBuilder.GetName(datasource.TableName).Substring(0, 4);
        BridgeName = $"_{tmp}";
    }

    private IDBMS Dbms { get; init; }

    public Action<string>? Logger { get; set; } = null;

    public int? Timeout { get; set; } = null;

    internal IDbConnection Connection { get; init; }

    internal Func<string, Destination> DestinationResolver { get; init; }

    private Action<SelectQuery>? Injector { get; init; }

    private Datasource Datasource { get; init; }

    internal string BridgeName { get; init; }

    private Destination Destination => (Datasource.Destination != null) ? Datasource.Destination : throw new InvalidProgramException();

    private VersioningConfig VersioningConfig { get; init; }

    private VersionConfig VersionConfig { get; init; }

    private OffsetConfig OffsetConfig { get; init; }

    private string RenewalIdColumn => $"{OffsetConfig.RenewalColumnPrefix}{Destination.Sequence.Column}";

    private string OffsetIdColumn => $"{OffsetConfig.OffsetColumnPrefix}{Destination.Sequence.Column}";

    private string OffsetRemarksColumn => $"{OffsetConfig.OffsetRemarksColumn}";

    private SelectQuery CreateSelectFromBridgeQuery()
    {
        var alias = "bridge";

        var sq = new SelectQuery();
        sq.From(BridgeName).As(alias);
        return sq;
    }

    private SelectQuery CreateSelectFromBridgeQueryAsOffset()
    {
        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, OffsetIdColumn).As(Destination.Sequence.Column);

        return sq;
    }

    private SelectQuery CreateSelectFromBridgeQueryAsRenewal()
    {
        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, RenewalIdColumn).As(Destination.Sequence.Column);

        //where
        sq.Where.Add().Column(bridge, RenewalIdColumn).IsNotNull();
        return sq;
    }

    private int ExecuteQuery(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var cnt = Connection.Execute(q, commandTimeout: Timeout);
        Logger?.Invoke($"count : {cnt}");

        return cnt;
    }

    private T ExecuteScalar<T>(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var val = Connection.ExecuteScalar<T>(q, commandTimeout: Timeout);
        Logger?.Invoke($"results : {val?.ToString()}");

        return val;
    }

    public Results Offset()
    {
        var sq = BuildSelectBridgeQuery();
        return Offset(sq);
    }

    private Results Offset(SelectQuery bridgequery)
    {
        var results = new Results();

        var cnt = CreateBridgeTable(bridgequery);
        if (cnt == 0) return results;

        //offset
        results.Add(OffsetMain(cnt));

        //renewal
        results.Add(RenewalMain());

        //common
        if (InsertOffsetMap() != cnt) throw new InvalidOperationException();
        results.Add(new Result() { Table = Datasource.GetOffsetTableName(), Count = cnt });

        if (InsertVersion(bridgequery) != 1) throw new InvalidOperationException();
        results.Add(new Result() { Table = Datasource.GetVersionTableName(), Count = 1 });

        //nest
        Datasource.OffsetExtensions.ForEach(x =>
        {
            //replace root table injector
            Action<SelectQuery> act = q => q.FromClause.TableName = BridgeName;

            var s = new InsertSynchronizer(this, x, act) { Logger = Logger }; ;
            results.Add(s.Insert());
        });
        return results;
    }

    private Results OffsetMain(int cnt)
    {
        var results = new Results() { Name = "offset" };

        if (InsertDestinationAsOffset() != cnt) throw new InvalidOperationException();
        results.Add(new Result() { Table = Destination.TableName, Count = cnt });

        if (DeleteKeyMap() != cnt) throw new InvalidOperationException();
        results.Add(new Result() { Table = Datasource.GetKeymapTableName(), Count = cnt });

        if (InsertSync(CreateSelectFromBridgeQueryAsOffset()) != cnt) throw new InvalidOperationException();
        results.Add(new Result() { Table = Datasource.GetSyncTableName(), Count = cnt });

        return results;
    }

    private Results RenewalMain()
    {
        var results = new Results() { Name = "renewal" };

        var cnt = InsertDestinationAsRenewal();
        results.Add(new Result() { Table = Destination.TableName, Count = cnt });

        if (InsertKeyMap() != cnt) throw new InvalidOperationException();
        results.Add(new Result() { Table = Datasource.GetKeymapTableName(), Count = cnt });

        if (InsertSync(CreateSelectFromBridgeQueryAsRenewal()) != cnt) throw new InvalidOperationException();
        results.Add(new Result() { Table = Datasource.GetSyncTableName(), Count = cnt });

        return results;
    }

    private SelectQuery BuildSelectBridgeQuery()
    {
        var sq = BuildSelectExpectQuery();
        sq = BuildSelectBridgeQuery(sq);
        return sq;
    }

    private SelectQuery BuildSelectExpectQuery()
    {
        /*
         * select d.*, m.datasource_id
         * from destination d
         * inner join keymap m on d.desitination_id = m.desitination_id
         * inner join sync s on m.desitination_id = s.destination_id
         * inner join version v on s.version_id = v.version_id
         * where 
         *     v.datasource_name = :dsname
         *     and injection
         */
        var alias = "d";

        var sq = new SelectQuery();
        var d = sq.From(Destination.TableName).As(alias);
        sq.SelectAll(d);

        // relation
        sq = InjectKeymap(sq);

        //inject from custom function
        if (Injector != null) Injector(sq);

        return sq;
    }

    private SelectQuery BuildSelectBridgeQuery(SelectQuery expectquery)
    {
        //TODO : InspectionIgnoreColumns

        /*
         * with
         * e as (
         *     --expectquery
         * ), 
         * select offset_id, renewal_id, d.*, remarks
         * from e
         * left join datasource d on e.datasource_id = d.datasource_id
         * where --deleted or changed
         */
        var expectalias = "e";
        var alias = "d";

        var sq = expectquery.PushToCommonTable(expectalias);
        var e = sq.From(expectalias);
        var d = e.LeftJoin(SqlParser.Parse(Datasource.Query)).As(alias).On(x =>
        {
            Datasource.KeyColumns.ForEach(col => x.Add().Equal(col.Key));
        });

        var cols = Destination.GetInsertColumns().ToList();

        var whereDeleted = (ConditionGroup g) =>
        {
            g.Add().Or().Column(d, Datasource.KeyColumns.First().Key).IsNull();
        };

        var whereChanged = (ConditionGroup g) =>
        {
            //value change
            //  epect.value <> d.value or not(expect.value is null and d.value is null)
            cols.ForEach(col =>
            {
                g.Add().Or().Column(e, col).NotEqual(d, col);
                g.AddGroup(g2 =>
                {
                    g2.Or().Not();
                    g2.Add().And().Column(e, col).IsNull();
                    g2.Add().And().Column(d, col).IsNull();
                });
            });
        };

        var selectOffsetId = (SelectItem item) =>
        {
            item.CaseWhen(w =>
            {
                w.Add().WhenGroup(g =>
                {
                    whereDeleted(g);
                    whereChanged(g);
                }).Then(Destination.Sequence.Command);
                w.Add().ElseNull();
            }).As(OffsetIdColumn);
        };

        var selectRenewalId = (SelectItem item) =>
        {
            item.CaseWhen(w =>
            {
                w.Add().WhenGroup(g =>
                {
                    whereChanged(g);
                }).Then(Destination.Sequence.Command);
                w.Add().ElseNull();
            }).As(RenewalIdColumn);
        };

        var selectRemarks = (SelectItem item) =>
        {
            item.Concat(Dbms.ConcatFunctionToken, Dbms.ConcatSplitToken, lst =>
            {
                //case when datasource_id is null then 'deleted' end
                lst.Add().CaseWhen(w =>
                {
                    w.Add().When(x => x.Column(e, Datasource.KeyColumns.First().Key).IsNull()).Then("'deleted'");
                });

                //case when a.val <> b.val or not(a.val is null and b.val is null) then 'val is changed' end
                cols.ForEach(col =>
                {
                    lst.Add().CaseWhen(w =>
                    {
                        w.Add().WhenGroup(g =>
                        {
                            g.Add().Or().Column(e, col).NotEqual(d, col);
                            g.AddGroup(g2 =>
                            {
                                g2.Or().Not();
                                g2.Add().And().Column(e, col).IsNull();
                                g2.Add().And().Column(d, col).IsNull();
                            });
                        }).Then($"'{col} is changed,'");
                    });
                });
            }).As(OffsetRemarksColumn);
        };

        //select
        InjectSelectVersion(sq);
        sq.Select(e, Destination.Sequence.Column);
        selectOffsetId(sq.Select.Add());
        selectRenewalId(sq.Select.Add());
        sq.SelectAll(d);
        selectRemarks(sq.Select.Add());

        //where
        sq.Where.Add().CaseWhen(w =>
        {
            w.Add().WhenGroup(g =>
            {
                whereDeleted(g);
                whereChanged(g);
            }).Then(true);
        });

        return sq;
    }

    private SelectQuery InjectSelectVersion(SelectQuery sq)
    {
        var alias = "v";

        var config = VersioningConfig;
        if (config == null) return sq;

        var seq = config.Sequence;

        var v = sq.With.Add(q =>
        {
            q.Select(seq.Command).As(seq.Column);
        }).As(alias);

        sq.FromClause.CrossJoin(v);
        sq.Select.Add().Column(alias, seq.Column);

        return sq;
    }

    private int InsertDestinationAsOffset()
    {
        /*
         * select bridge.offset_id as id, d.value1, d.value2 * -1 as value2
         * from tmp bridge
         * inner join destination on bridge.destination_id = d.destination_id
         */
        var alias = "d";

        //from
        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;
        var origin = bridge.InnerJoin(Destination.TableName).As(alias).On(Destination.Sequence.Column);

        var selectOffsetColumns = () =>
        {
            var cols = Destination.GetInsertColumns().ToList();
            cols.ForEach(x =>
            {
                if (OffsetConfig.SignInversionColumns.Contains(x))
                {
                    sq.Select.Add().Value($"{origin.AliasName}.{x} * -1").As(x);
                }
                else
                {
                    sq.Select.Add().Column(origin.AliasName, x).As(x);
                }
            });
        };

        //select
        sq.Select(bridge, OffsetIdColumn).As(Destination.Sequence.Column);
        selectOffsetColumns();

        var q = sq.ToInsertQuery(Destination.TableName);
        return ExecuteQuery(q);
    }

    private int InsertDestinationAsRenewal()
    {
        /*
         * select renewal_id as id, value1, value2
         * from tmp bridge
         * where
         *     renewal_id is not null
         */
        //from
        var sq = CreateSelectFromBridgeQueryAsRenewal();
        var bridge = sq.FromClause;

        //select
        var cols = Destination.GetInsertColumns().ToList();
        cols.ForEach(x =>
        {
            sq.Select.Add().Column(bridge, x).As(x);
        });

        var q = sq.ToInsertQuery(Destination.TableName);
        return ExecuteQuery(q);
    }

    private int InsertOffsetMap()
    {
        /*
          * select destination_id, offset_id, renewal_id, remarks
          * from bridge
          */

        var offset = Datasource.GetOffsetTableName();
        if (offset == null) throw new InvalidProgramException();

        //from
        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, Destination.Sequence.Column);
        sq.Select(bridge, OffsetIdColumn);
        sq.Select(bridge, RenewalIdColumn);
        sq.Select(bridge, OffsetRemarksColumn);

        var q = sq.ToInsertQuery(offset);
        return ExecuteQuery(q);
    }

    private int InsertVersion(SelectQuery bridgequery)
    {
        /*
         * insert into version (version_id, datasource_name, bridge_command)
         * select distinct 
         *     version_id
         *     , :name as datasource_name
         *     , :query as bridge_command
         * from tmp bridge
         */
        var ver = Datasource.GetVersionTableName();
        if (ver == null) throw new InvalidProgramException();

        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;

        //select
        sq.Distinct();
        sq.Select(bridge, VersioningConfig.Sequence.Column);
        sq.Select(":name").As(VersionConfig.DatasourceNameColumn).Parameter(":name", Datasource.TableName);
        sq.Select(":query").As(VersionConfig.BridgeCommandColumn).Parameter(":query", bridgequery.ToQuery().CommandText);

        var q = sq.ToInsertQuery(ver);
        return ExecuteQuery(q);
    }

    private int InsertSync(SelectQuery selectQuery)
    {
        /*
         * insert into sync (destination_id, version_id)
         * select 
         *     X as destination_id
         *     , version_id
         * from tmp bridge
         */
        var sync = Datasource.GetSyncTableName();
        if (sync == null) throw new InvalidProgramException();

        var bridge = selectQuery.FromClause;

        //select
        selectQuery.Select(bridge, VersioningConfig.Sequence.Column);

        var q = selectQuery.ToInsertQuery(sync);
        return ExecuteQuery(q);
    }

    private int InsertKeyMap()
    {
        /*
         * insert into map (destination_id, datasource_id)
         * select 
         *     renewal_id as destination_id
         *     , datasource_id
         * from tmp bridge
         */

        var map = Datasource.GetKeymapTableName();
        if (map == null) throw new InvalidProgramException();

        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, RenewalIdColumn).As(Destination.Sequence.Column);
        Datasource.KeyColumns.ForEach(x => sq.Select(bridge, x.Key));

        //where
        sq.Where.Add().Column(bridge, RenewalIdColumn).IsNotNull();

        var q = sq.ToInsertQuery(map);
        return ExecuteQuery(q);
    }

    private int DeleteKeyMap()
    {
        /*
         * delete from map
         * where 
         *   exists (select * from tmp bridge where bridge.destination_id = map.destination_id)
         */

        var map = Datasource.GetKeymapTableName();
        if (map == null) throw new InvalidProgramException();

        var w = new ConditionClause("where");
        w.ConditionGroup.Add().Exists(x =>
        {
            x.SelectAll();
            var bridge = x.From(BridgeName).As("bridge");
            var id = Destination.Sequence.Column;
            x.Where.Add().Column(bridge, id).Equal(map, id);
        });

        var q = w.ToQuery();
        q.CommandText = $"delete from {map} {q.CommandText}";

        return ExecuteQuery(q);
    }

    private int CreateBridgeTable(SelectQuery sq)
    {
        var q = sq.ToCreateTableQuery(BridgeName, true);
        ExecuteQuery(q);

        q = BuildBridgeCountQuery();
        return ExecuteScalar<int>(q);
    }

    private Query BuildBridgeCountQuery()
    {
        var sq = new SelectQuery();
        sq.From(BridgeName);
        sq.SelectCount();
        return sq.ToQuery();
    }

    private SelectQuery InjectKeymap(SelectQuery sq)
    {
        /* 
         * select d.*, m.datasource_id
         * from destination d
         * inner join keymap m on d.desitination_id = _m.desitination_id
         * inner join sync s on _m.desitination_id = _s.destination_id
         * inner join version v on _s.version_id = _v.version_id
         * where
         * v.datasource_name = :dsname
         */
        var keymapalias = "m";
        var syncalias = "s";
        var veralias = "v";

        //relation
        var keymap = Datasource.GetKeymapTableName();
        if (keymap == null) throw new InvalidProgramException();
        var m = sq.FromClause.InnerJoin(keymap).As(keymapalias).On(Destination.Sequence.Column);

        var sync = Datasource.GetSyncTableName();
        if (sync == null) throw new InvalidProgramException();
        var s = m.InnerJoin(sync).As(syncalias).On(Destination.Sequence.Column);

        var ver = Datasource.GetVersionTableName();
        if (ver == null) throw new InvalidProgramException();
        var v = s.InnerJoin(ver).As(veralias).On(VersioningConfig.Sequence.Column);

        //select
        Datasource.KeyColumns.ForEach(x => sq.Select(m, x.Key));

        //where
        sq.Where.Add().Column(v, VersionConfig.DatasourceNameColumn).Equal(":dsname").Parameter(":dsname", Datasource.TableName);

        return sq;
    }
}