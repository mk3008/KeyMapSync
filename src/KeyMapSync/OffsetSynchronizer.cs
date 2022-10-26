using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using SqModel;
using SqModel.Analysis;
using SqModel.Dapper;
using SqModel.Expression;
using System.Data;
using System.Reflection.PortableExecutable;
using Utf8Json;
using Utf8Json.Resolvers;

namespace KeyMapSync;

public class OffsetSynchronizer
{
    public OffsetSynchronizer(SystemConfig config, IDbConnection connection, Datasource datasource, Action<SelectQuery, Datasource, string>? injector = null)
    {
        SystemConfig = config;
        Connection = connection;
        Datasource = datasource;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(String.Concat(Destination.TableFulleName, '_', datasource.DatasourceName)).Substring(0, 4);
        BridgeName = $"_{tmp}";

        IsRoot = true;

        BridgeQuery = BuildSelectBridgeQuery();
    }

    public SystemConfig SystemConfig { get; init; }

    public Action<string>? Logger { get; set; } = null;

    public string Argument { get; set; } = String.Empty;

    private bool IsRoot { get; init; }

    internal IDbConnection Connection { get; init; }

    private Action<SelectQuery, Datasource, string>? Injector { get; init; }

    private Datasource Datasource { get; init; }

    internal string BridgeName { get; set; }

    private SelectQuery BridgeQuery { get; set; }

    private Destination Destination => Datasource.Destination;

    private KeyMapConfig KeyMapConfig => SystemConfig.KeyMapConfig;

    private string MapTableName => Datasource.GetKeymapTableName(KeyMapConfig);

    private SyncConfig SyncConfig => SystemConfig.SyncConfig;

    private string SyncTableName => Destination.GetSyncTableName(SyncConfig);

    private CommandConfig CommandConfig => SystemConfig.CommandConfig;

    private OffsetConfig OffsetConfig => SystemConfig.OffsetConfig;

    public Result Execute()
    {
        //kms_transaction start
        var rep = new TransactionRepository(Connection) { Logger = Logger };
        var tranid = rep.Insert(Datasource, Argument);
        Logger?.Invoke($"--transaction_id : {tranid}");

        //main
        var result = Execute(tranid);
        result.Caption = $"offset";
        result.TransactionId = tranid;

        //kms_transaction end
        var text = JsonSerializer.ToJsonString(result, StandardResolver.ExcludeNull);
        rep.Update(tranid, text);

        return result;
    }

    internal Result Execute(long tranid)
    {
        Logger?.Invoke($"--offset {Destination.TableFulleName} <- {Datasource.DatasourceName}");

        var result = new Result();

        var cnt = CreateBridgeTable();
        if (cnt == 0) return result;

        //mapping
        result.Add(RefreshKeyMap());

        //nest
        //offset
        result.Add(InsertOffset(tranid));

        //renew
        result.Add(InsertRenew(tranid));

        //Datasource.Extensions.ForEach(nestdatasource =>
        //{
        //    //replace root table injector
        //    Action<SelectQuery, string> replaceRootTable = (q, _) => q.FromClause.TableName = BridgeName;

        //    var s = new InsertSynchronizer(this, nestdatasource, replaceRootTable) { Logger = Logger };
        //    result.Add(s.Execute(tranid));
        //});

        return result;
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
         * select d.*, datasource_ids
         * from destination d
         * inner join systemtable
         * where 
         *     injection
         */
        var alias = "d";

        var sq = new SelectQuery();
        var d = sq.From(Destination.TableName).As(alias);
        sq.SelectAll(d);

        sq = InjectSelectDatasourceId(sq);

        //inject from custom function
        if (Injector != null) Injector(sq, Datasource, Argument);

        return sq;
    }

    private SelectQuery InjectSelectDatasourceId(SelectQuery sq)
    {
        /*
         * select m.datasource_ids
         * from destination dinner join keymap _m on d.desitination_id = _m.desitination_id
         * inner join sync _s on d.desitination_id = _s.destination_id
         * inner join kms_processes _p on _s.kms_process_id = _p.kms_process_id
         * inner join kms_transactions _t on _p.kms_transaction_id = _t.kms_transaction_id
         * where 
         *     _t.destinatiom_id = :destinatiom_id
         *     and _t.datasource_id = :datasource_id
         */
        var seq = Destination.SequenceConfig.Column;

        var d = sq.FromClause;
        var m = d.InnerJoin(MapTableName).As("_m").On(seq);
        var s = d.InnerJoin(SyncTableName).As("_s").On(seq);
        var p = s.InnerJoin("kms_processes").As("_p").On("kms_process_id");
        var t = p.InnerJoin("kms_transactions").As("_t").On("kms_transaction_id");

        Datasource.KeyColumnsConfig.ForEach(x => sq.Select.Add().Column(m, x.Key));

        sq.Where.Add().Column(t, "destination_id").Equal(":destination_id").AddParameter(":destination_id", Destination.DestinationId);
        sq.Where.Add().Column(t, "datasource_id").Equal(":datasource_id").AddParameter(":datasource_id", Datasource.DatasourceId);

        return sq;
    }

    private SelectQuery BuildSelectBridgeQuery(SelectQuery expectquery)
    {
        //TODO : InspectionIgnoreColumns

        /*
         * with
         * e as (
         *     expect query
         * ), 
         * select e.destination_id, offset_id, renewal_id, d.*, remarks
         * from e
         * left join datasource d on e.datasource_ids = d.datasource_ids
         * where deleted or changed
         */
        var sq = expectquery.PushToCommonTable("expect");
        var e = sq.From("expect").As("e");
        var seq = Destination.SequenceConfig;

        var d = e.LeftJoin(SqlParser.Parse(Datasource.Query)).As("d").On(x =>
        {
            Datasource.KeyColumnsConfig.ForEach(col => x.Add().Equal(col.Key));
        });

        var dscols = SqlParser.Parse(Datasource.Query).Select.GetColumnNames();
        var ignores = Destination.InspectionIgnoreColumns;
        var cols = Destination.GetInsertColumns().ToList()
            .Where(x => dscols.Contains(x))
            .Where(x => !ignores.Contains(x)).ToList();

        var whereDeleted = (ConditionGroup g) =>
        {
            g.Add().Or().Column(d, Datasource.KeyColumnsConfig.First().Key).IsNull();
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
                }).Then(seq.Command);
                w.Add().ElseNull();
            }).As(Destination.GetOffsetIdColumnName(OffsetConfig));
        };

        var selectRenewalId = (SelectItem item) =>
        {
            item.CaseWhen(w =>
            {
                w.Add().WhenGroup(g =>
                {
                    whereChanged(g);
                }).Then(seq.Command);
                w.Add().ElseNull();
            }).As(Destination.GetRenewalIdColumnName(OffsetConfig));
        };

        var selectRemarks = (SelectItem item) =>
        {
            item.Concat("concat", ",", lst =>
            {
                //case when datasource_id is null then 'deleted' end
                lst.Add().CaseWhen(w =>
                {
                    w.Add().When(x => x.Column(d, Datasource.KeyColumnsConfig.First().Key).IsNull()).Then("'deleted'");
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
            }).As(OffsetConfig.OffsetRemarksColumn);
        };

        //select expect.destination_id, offset_id, renewal_id, actual.*, remarks
        sq.Select(e, seq.Column);
        selectOffsetId(sq.Select.Add());
        selectRenewalId(sq.Select.Add());
        sq.SelectAll(d);
        selectRemarks(sq.Select.Add());

        //where deleted or changed
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

    private int CreateBridgeTable()
    {
        var q = BridgeQuery.ToCreateTableQuery(BridgeName, true);
        ExecuteQuery(q);

        q = BuildBridgeCountQuery();
        return ExecuteScalar<int>(q);
    }

    private int ExecuteQuery(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var cnt = Connection.Execute(q, commandTimeout: CommandConfig.Timeout);
        Logger?.Invoke($"count : {cnt}");

        return cnt;
    }

    private Query BuildBridgeCountQuery()
    {
        var sq = new SelectQuery();
        sq.From(BridgeName);
        sq.SelectCount();
        return sq.ToQuery();
    }

    private T ExecuteScalar<T>(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var val = Connection.ExecuteScalar<T>(q, commandTimeout: CommandConfig.Timeout);
        Logger?.Invoke($"results : {val?.ToString()}");

        return val;
    }

    private Result RefreshKeyMap()
    {
        var result = new Result() { Caption = "refresh keymap" };
        result.Add(InsertOffsetMap());
        result.Add(DeleteKeyMap());
        return result;
    }

    private Result InsertOffsetMap()
    {
        /*
          * select destination_id, offset_id, renewal_id, remarks
          * from bridge
          */

        var offset = Destination.GetOffsetTableName(OffsetConfig);

        //from bridge
        var sq = new SelectQuery();
        var bridge = sq.From(BridgeName).As("bridge");

        //select destination_id, offset_id, renewal_id, remarks
        sq.Select(bridge, Destination.SequenceConfig.Column);
        sq.Select(bridge, Destination.GetOffsetIdColumnName(OffsetConfig));
        sq.Select(bridge, Destination.GetRenewalIdColumnName(OffsetConfig));
        sq.Select(bridge, OffsetConfig.OffsetRemarksColumn);

        var q = sq.ToInsertQuery(offset, new());
        var cnt = ExecuteQuery(q);
        return new Result() { Table = offset, Count = cnt };
    }

    private Result DeleteKeyMap()
    {
        /*
         * delete from keymap
         * where 
         *   exists (select * from bridge where bridge.destination_id = map.destination_id)
         */

        var map = Datasource.GetKeymapTableName(KeyMapConfig);
        if (map == null) throw new InvalidProgramException("keymaptable is not found.");

        var w = new ConditionClause("where");
        w.ConditionGroup.Add().Exists(x =>
        {
            x.SelectAll();
            var bridge = x.From(BridgeName).As("bridge");
            var id = Destination.SequenceConfig.Column;
            x.Where.Add().Column(bridge, id).Equal(map, id);
        });

        var q = w.ToQuery();
        q.CommandText = $"delete from {map} {q.CommandText}";

        var cnt = ExecuteQuery(q);
        return new Result() { Table = map, Count = cnt, Command = "delete" };
    }

    private Result InsertOffset(long tranid)
    {
        //virtual datasource
        var ds = new Datasource()
        {
            DatasourceId = Datasource.DatasourceId,
            DatasourceName = Datasource.DatasourceName,
            Destination = Datasource.Destination,
            DestinationId = Datasource.Destination.DestinationId,
            Query = GetSelectOffsetDatasourceQuery()
        };
        var s = new InsertSynchronizer(this, ds) { Logger = Logger };
        var r = s.Execute(tranid);
        r.Caption = "insert offset";
        return r;
    }

    private string GetSelectOffsetDatasourceQuery()
    {
        /*
         * select bridge.offset_id as destination_id, e.value1, e.value2 * -1 as value2
         * from bridge
         * inner join destination expect on bridge.destination_id = expect.destination_id
         * inner join extension1 expect_ext1 on expect.? = expect_ext1.?
         */

        //from
        var sq = new SelectQuery();
        var bridge = sq.From(BridgeName).As("bridge");
        var e = bridge.InnerJoin(Destination.TableName).As("e").On(Destination.SequenceConfig.Column);

        var addSelectColumn = () =>
        {
            var dscols = SqlParser.Parse(Datasource.Query).Select.GetColumnNames();
            var cols = Destination.GetInsertColumns().Where(x => dscols.Contains(x)).ToList();
            cols.ForEach(x =>
            {
                if (Destination.SignInversionColumns.Contains(x))
                {
                    sq.Select.Add().Value($"{e.AliasName}.{x} * -1").As(x);
                }
                else
                {
                    sq.Select.Add().Column(e.AliasName, x).As(x);
                }
            });
        };

        var addSelectHeaderColumn = () =>
        {
            //header
            var header = Destination.HeaderDestination;
            if (header == null) return;
            var h = e.InnerJoin(header.TableFulleName).As("h").On(header.SequenceConfig.Column);
            var dscols = sq.GetSelectItems().Select(x => x.Name);
            header.GetInsertColumns().Where(x => !dscols.Contains(x)).ToList()
                .ForEach(x => sq.Select.Add().Column(h, x));
        };

        //select
        sq.Select(bridge, Destination.GetOffsetIdColumnName(OffsetConfig)).As(Destination.SequenceConfig.Column);
        addSelectColumn();
        addSelectHeaderColumn();
        var q = sq.ToQuery();
        return q.CommandText;
    }

    private Result InsertRenew(long tranid)
    {
        //virtual datasource
        var ds = new Datasource()
        {
            DatasourceId = Datasource.DatasourceId,
            DatasourceName = Datasource.DatasourceName,
            Destination = Datasource.Destination,
            DestinationId = Datasource.Destination.DestinationId,
            Query = GetSelectRenewalDatasourceQuery()
        };
        var s = new InsertSynchronizer(this, ds, "r") { Logger = Logger };
        var r = s.Execute(tranid);
        r.Caption = "insert renew";
        return r;
    }

    private string GetSelectRenewalDatasourceQuery()
    {
        /*
         * select bridge.renewal_id as destination_id, b.*
         * from bridge
         * where renewal_id is not null
         */

        //from
        var sq = new SelectQuery();
        var bridge = sq.From(BridgeName).As("bridge");

        var addSelectColumn = () =>
        {
            var dscols = SqlParser.Parse(Datasource.Query).Select.GetColumnNames();
            dscols.ForEach(x =>
            {
                sq.Select.Add().Column(bridge, x);
            });
        };

        //select
        var renewIdName = Destination.GetRenewalIdColumnName(OffsetConfig);
        sq.Select(bridge, renewIdName).As(Destination.SequenceConfig.Column);
        addSelectColumn();

        //select
        //sq.SelectAll(bridge);

        //where
        sq.Where.Add().Column(bridge, renewIdName).IsNotNull();

        var q = sq.ToQuery();
        return q.CommandText;
    }

    //private SelectQuery CreateSelectFromBridgeQueryAsOffset()
    //{
    //    var sq = CreateSelectFromBridgeQuery();
    //    var bridge = sq.FromClause;

    //    //select
    //    sq.Select(bridge, OffsetIdColumn).As(Destination.Sequence.Column);

    //    return sq;
    //}

    //private SelectQuery CreateSelectFromBridgeQueryAsRenewal()
    //{
    //    var sq = CreateSelectFromBridgeQuery();
    //    var bridge = sq.FromClause;

    //    //select
    //    sq.Select(bridge, RenewalIdColumn).As(Destination.Sequence.Column);

    //    //where
    //    sq.Where.Add().Column(bridge, RenewalIdColumn).IsNotNull();
    //    return sq;
    //}





    //public Results Offset()
    //{
    //    var sq = BuildSelectBridgeQuery();
    //    return Offset(sq);
    //}

    //private Results Offset(SelectQuery bridgequery)
    //{
    //    var results = new Results();

    //    var cnt = CreateBridgeTable(bridgequery);
    //    if (cnt == 0) return results;

    //    //offset
    //    results.Add(OffsetMain(cnt));

    //    //renewal
    //    results.Add(RenewalMain());

    //    //common
    //    if (InsertOffsetMap() != cnt) throw new InvalidOperationException();
    //    results.Add(new Result() { Table = Datasource.GetOffsetTableName(), Count = cnt });

    //    if (InsertVersion(bridgequery) != 1) throw new InvalidOperationException();
    //    results.Add(new Result() { Table = Datasource.GetVersionTableName(), Count = 1 });

    //    //nest
    //    Datasource.OffsetExtensions.ForEach(x =>
    //    {
    //        //replace root table injector
    //        Action<SelectQuery> act = q => q.FromClause.TableName = BridgeName;

    //        var s = new InsertSynchronizer(this, x, act) { Logger = Logger }; ;
    //        results.Add(s.Insert());
    //    });
    //    return results;
    //}

    //private Results OffsetMain(int cnt)
    //{
    //    var results = new Results() { Name = "offset" };

    //    if (InsertDestinationAsOffset() != cnt) throw new InvalidOperationException();
    //    results.Add(new Result() { Table = Destination.TableName, Count = cnt });

    //    if (DeleteKeyMap() != cnt) throw new InvalidOperationException();
    //    results.Add(new Result() { Table = Datasource.GetKeymapTableName(), Count = cnt });

    //    if (InsertSync(CreateSelectFromBridgeQueryAsOffset()) != cnt) throw new InvalidOperationException();
    //    results.Add(new Result() { Table = Datasource.GetSyncTableName(), Count = cnt });

    //    return results;
    //}

    //private Results RenewalMain()
    //{
    //    var results = new Results() { Name = "renewal" };

    //    var cnt = InsertDestinationAsRenewal();
    //    results.Add(new Result() { Table = Destination.TableName, Count = cnt });

    //    if (InsertKeyMap() != cnt) throw new InvalidOperationException();
    //    results.Add(new Result() { Table = Datasource.GetKeymapTableName(), Count = cnt });

    //    if (InsertSync(CreateSelectFromBridgeQueryAsRenewal()) != cnt) throw new InvalidOperationException();
    //    results.Add(new Result() { Table = Datasource.GetSyncTableName(), Count = cnt });

    //    return results;
    //}







    //private SelectQuery InjectSelectVersion(SelectQuery sq)
    //{
    //    var alias = "v";

    //    var config = VersioningConfig;
    //    if (config == null) return sq;

    //    var seq = config.Sequence;

    //    var v = sq.With.Add(q =>
    //    {
    //        q.Select(seq.Command).As(seq.Column);
    //    }).As(alias);

    //    sq.FromClause.CrossJoin(v);
    //    sq.Select.Add().Column(alias, seq.Column);

    //    return sq;
    //}

    //private int InsertDestinationAsOffset()
    //{
    //    /*
    //     * select bridge.offset_id as id, d.value1, d.value2 * -1 as value2
    //     * from tmp bridge
    //     * inner join destination on bridge.destination_id = d.destination_id
    //     */
    //    var alias = "d";

    //    //from
    //    var sq = CreateSelectFromBridgeQuery();
    //    var bridge = sq.FromClause;
    //    var origin = bridge.InnerJoin(Destination.TableName).As(alias).On(Destination.Sequence.Column);

    //    var selectOffsetColumns = () =>
    //    {
    //        var cols = Destination.GetInsertColumns().ToList();
    //        cols.ForEach(x =>
    //        {
    //            if (OffsetConfig.SignInversionColumns.Contains(x))
    //            {
    //                sq.Select.Add().Value($"{origin.AliasName}.{x} * -1").As(x);
    //            }
    //            else
    //            {
    //                sq.Select.Add().Column(origin.AliasName, x).As(x);
    //            }
    //        });
    //    };

    //    //select
    //    sq.Select(bridge, OffsetIdColumn).As(Destination.Sequence.Column);
    //    selectOffsetColumns();

    //    var q = sq.ToInsertQuery(Destination.TableName);
    //    return ExecuteQuery(q);
    //}

    //private int InsertDestinationAsRenewal()
    //{
    //    /*
    //     * select renewal_id as id, value1, value2
    //     * from tmp bridge
    //     * where
    //     *     renewal_id is not null
    //     */
    //    //from
    //    var sq = CreateSelectFromBridgeQueryAsRenewal();
    //    var bridge = sq.FromClause;

    //    //select
    //    var cols = Destination.GetInsertColumns().ToList();
    //    cols.ForEach(x =>
    //    {
    //        sq.Select.Add().Column(bridge, x).As(x);
    //    });

    //    var q = sq.ToInsertQuery(Destination.TableName);
    //    return ExecuteQuery(q);
    //}



    //private int InsertVersion(SelectQuery bridgequery)
    //{
    //    /*
    //     * insert into version (version_id, datasource_name, bridge_command)
    //     * select distinct 
    //     *     version_id
    //     *     , :name as datasource_name
    //     *     , :query as bridge_command
    //     * from tmp bridge
    //     */
    //    var ver = Datasource.GetVersionTableName();
    //    if (ver == null) throw new InvalidProgramException();

    //    var sq = CreateSelectFromBridgeQuery();
    //    var bridge = sq.FromClause;

    //    //select
    //    sq.Distinct();
    //    sq.Select(bridge, VersioningConfig.Sequence.Column);
    //    sq.Select(":name").As(VersionConfig.DatasourceNameColumn).Parameter(":name", Datasource.TableName);
    //    sq.Select(":query").As(VersionConfig.BridgeCommandColumn).Parameter(":query", bridgequery.ToQuery().CommandText);

    //    var q = sq.ToInsertQuery(ver);
    //    return ExecuteQuery(q);
    //}

    //private int InsertSync(SelectQuery selectQuery)
    //{
    //    /*
    //     * insert into sync (destination_id, version_id)
    //     * select 
    //     *     X as destination_id
    //     *     , version_id
    //     * from tmp bridge
    //     */
    //    var sync = Datasource.GetSyncTableName();
    //    if (sync == null) throw new InvalidProgramException();

    //    var bridge = selectQuery.FromClause;

    //    //select
    //    selectQuery.Select(bridge, VersioningConfig.Sequence.Column);

    //    var q = selectQuery.ToInsertQuery(sync);
    //    return ExecuteQuery(q);
    //}

    //private int InsertKeyMap()
    //{
    //    /*
    //     * insert into map (destination_id, datasource_id)
    //     * select 
    //     *     renewal_id as destination_id
    //     *     , datasource_id
    //     * from tmp bridge
    //     */

    //    var map = Datasource.GetKeymapTableName();
    //    if (map == null) throw new InvalidProgramException();

    //    var sq = CreateSelectFromBridgeQuery();
    //    var bridge = sq.FromClause;

    //    //select
    //    sq.Select(bridge, RenewalIdColumn).As(Destination.Sequence.Column);
    //    Datasource.KeyColumns.ForEach(x => sq.Select(bridge, x.Key));

    //    //where
    //    sq.Where.Add().Column(bridge, RenewalIdColumn).IsNotNull();

    //    var q = sq.ToInsertQuery(map);
    //    return ExecuteQuery(q);
    //}




    //private Query BuildBridgeCountQuery()
    //{
    //    var sq = new SelectQuery();
    //    sq.From(BridgeName);
    //    sq.SelectCount();
    //    return sq.ToQuery();
    //}


}