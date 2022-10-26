using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using SqModel;
using SqModel.Analysis;
using SqModel.Dapper;
using System.Data;
using Utf8Json;
using Utf8Json.Resolvers;

namespace KeyMapSync;

public class InsertSynchronizer
{
    internal InsertSynchronizer(OffsetSynchronizer owner, Datasource datasource, string temporarySufix = "")
    {
        Connection = owner.Connection;
        SystemConfig = owner.SystemConfig;
        Datasource = datasource;
        Injector = null;

        var tmp = BridgeNameBuilder.GetName(datasource.TableName).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}{temporarySufix}";

        IsRoot = false;

        BridgeQuery = BuildSelectBridgeQuery();
    }

    private InsertSynchronizer(InsertSynchronizer owner, Datasource datasource, Action<SelectQuery, Datasource, string> injector)
    {
        BaseDatasource = owner.Datasource;
        Connection = owner.Connection;
        SystemConfig = owner.SystemConfig;
        Datasource = datasource;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(String.Concat(Destination.TableFulleName, '_', datasource.DatasourceName)).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}";

        IsRoot = false;

        BridgeQuery = BuildSelectBridgeQuery();
    }

    public InsertSynchronizer(SystemConfig config, IDbConnection connection, Datasource datasource, Action<SelectQuery, Datasource, string>? injector = null)
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

    private IDbConnection Connection { get; init; }

    private Action<SelectQuery, Datasource, string>? Injector { get; init; }

    private Datasource? BaseDatasource { get; init; } = null;

    private Datasource Datasource { get; init; }

    private string BridgeName { get; set; }

    private SelectQuery BridgeQuery { get; set; }

    private Destination Destination => Datasource.Destination;

    private KeyMapConfig KeyMapConfig => SystemConfig.KeyMapConfig;

    private string MapTableName => Datasource.GetKeymapTableName(KeyMapConfig);

    private SyncConfig SyncConfig => SystemConfig.SyncConfig;

    private string SyncTableName => Destination.GetSyncTableName(SyncConfig);

    private ExtendConfig ExtendConfig => SystemConfig.ExtendConfig;

    private string? ExtTableName => BaseDatasource?.Destination.GetExtendTableName(ExtendConfig);

    private CommandConfig CommandConfig => SystemConfig.CommandConfig;

    private SelectQuery BuildSelectSequenceFromBridge()
    {
        var alias = "bridge";

        var sq = new SelectQuery();
        sq.From(BridgeName).As(alias);

        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, Destination.SequenceConfig.Column);

        return sq;
    }

    private int ExecuteQuery(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var cnt = Connection.Execute(q, commandTimeout: CommandConfig.Timeout);
        Logger?.Invoke($"--count : {cnt}");

        return cnt;
    }

    private T ExecuteScalar<T>(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var val = Connection.ExecuteScalar<T>(q, commandTimeout: CommandConfig.Timeout);
        Logger?.Invoke($"--results : {val?.ToString()}");

        return val;
    }

    public Result Execute()
    {
        //kms_transaction start
        var rep = new TransactionRepository(Connection) { Logger = Logger };
        var tranid = rep.Insert(Datasource, Argument);
        Logger?.Invoke($"--transaction_id : {tranid}");

        //main
        var result = Execute(tranid);
        result.Caption = $"insert";
        result.TransactionId = tranid;

        //kms_transaction end
        var text = JsonSerializer.ToJsonString(result, StandardResolver.ExcludeNull);
        rep.Update(tranid, text);

        return result;
    }

    internal Result Execute(long tranid)
    {
        Logger?.Invoke($"--insert {Destination.TableFulleName} <- {Datasource.DatasourceName}");

        var result = new Result();
        result.Caption = $"cascade insert";

        var cnt = CreateBridgeTable();
        if (cnt == 0) return result;

        //inject header insert
        var hs = BuildHeaderSyncronizer();
        if (hs != null)
        {
            var r = hs.Execute(tranid);
            r.Caption = "header inesrt";
            result.Add(r);

            var oldname = BridgeName;
            OverideBridgeNameAndQueryByHeader(hs.BridgeName);
            Logger?.Invoke($"*override bridge (old : {oldname}, new : {BridgeName})");

            var hcnt = CreateBridgeTable();
            if (cnt != hcnt) throw new Exception();
        }

        //process
        var procid = InsertProcessAndGetId(tranid);
        result.Add(new Result() { Table = "kms_processes", Count = cnt });

        //destination
        if (InsertDestination() != cnt) throw new InvalidOperationException();
        result.Add(new Result() { Table = Destination.TableFulleName, Count = cnt });

        //map
        if (IsRoot && !string.IsNullOrEmpty(Datasource.MapName))
        {
            if (InsertKeyMap() != cnt) throw new InvalidOperationException();
            result.Add(new Result() { Table = MapTableName, Count = cnt });
        }

        //sync
        if (InsertSync(procid) != cnt) throw new InvalidOperationException();
        result.Add(new Result() { Table = SyncTableName, Count = cnt });

        //ext
        if (Destination.IsHeader == false && BaseDatasource != null && ExtTableName != null)
        {
            if (InsertExt(procid) != cnt) throw new InvalidOperationException();
            result.Add(new Result() { Table = ExtTableName, Count = cnt });
        }

        //nest
        Datasource.Extensions.ForEach(nestdatasource =>
        {
            //replace root table injector
            Action<SelectQuery, Datasource, string> replaceRootTable = (q, _, _) => q.FromClause.TableName = BridgeName;

            var s = new InsertSynchronizer(this, nestdatasource, replaceRootTable) { Logger = Logger };
            result.Add(s.Execute(tranid));
        });
        return result;
    }

    private InsertSynchronizer? BuildHeaderSyncronizer()
    {
        var h = Destination.HeaderDestination;
        if (h == null) return null;

        //sync header
        Action<SelectQuery, Datasource, string> replaceRootTable = (q, _, _) => q.FromClause.TableName = BridgeName;
        var ds
            = new Datasource()
            {
                DatasourceId = Datasource.DatasourceId,
                DatasourceName = Datasource.DatasourceName,
                Destination = h,
                DestinationId = h.DestinationId,
                Query = h.Query
            };
        return new InsertSynchronizer(this, ds, replaceRootTable) { Logger = Logger };
    }

    private SelectQuery BuildSelectBridgeQuery()
    {
        /*
         * with
         * d as (
         *     --datasource
         * )
         * select 
         *     generate_sequence as destination_id
         *     , d.*
         * from d
         * left join map m on d.destination_id = m.destination_id
         * where m.destination_id is null
         */
        var alias = "d";

        var sq = SqlParser.Parse(Datasource.Query);

        //auto fix columns
        if (Datasource.KeyColumnsConfig.Any())
        {
            var f = sq.FromClause;
            var c = sq.GetSelectItems().Select(x => x.Name).ToList();
            var keys = Datasource.KeyColumnsConfig.Select(x => x.Key).ToList();
            keys.Where(x => !c.Contains(x)).ToList().ForEach(x => sq.Select.Add().Column(f, x));
        }
        if (Destination.IsHeader == false && BaseDatasource != null)
        {
            var f = sq.FromClause;
            var baseseq = BaseDatasource.Destination.SequenceConfig;
            sq.Select.Add().Column(f, baseseq.Column).As($"base_{baseseq.Column}");
        }

        var cols = sq.Select.GetColumnNames();

        //inject from custom function
        if (Injector != null) Injector(sq, Datasource, Argument);

        sq = sq.PushToCommonTable(alias);

        //from
        var d = sq.From(alias);

        //select
        var seq = Destination.SequenceConfig;
        if (!cols.Contains(seq.Column))
        {
            sq.Select(seq.Command).As(seq.Column);
        }

        cols.ForEach(x => sq.Select.Add().Column(d, x));

        //inject from config
        if (IsRoot && Datasource.HasKeymap) sq = InjectNotSyncCondition(sq);

        return sq;
    }

    private void OverideBridgeNameAndQueryByHeader(string headerbridge)
    {
        /*
         * select 
         *     h.sequence_column
         *     , d.*
         * from current_bridge d
         * inner join header_bridge h m on header_key_colums
         */
        var header = Destination.HeaderDestination;
        if (header == null) throw new Exception();

        var sq = new SelectQuery();
        var d = sq.From(BridgeName).As("d");
        var h = d.InnerJoin(headerbridge).As("h").On(x =>
        {
            header.KeyColumns.ToList().ForEach(key =>
            {
                x.Add().Column(x.LeftTable, key).Equal(x.RightTable, key);
            });
        });

        sq.Select.Add().Column(h, header.SequenceConfig.Column);

        BridgeQuery.GetSelectItems().Select(x => (!string.IsNullOrEmpty(x.Name)) ? x.Name : x.ColumnName).ToList().ForEach(x =>
        {
            sq.Select.Add().Column(d, x);
        });

        //override bridge table
        var tmp = BridgeNameBuilder.GetName(String.Concat(Destination.TableFulleName, '_', Datasource.DatasourceName)).Substring(0, 4);
        BridgeName = $"{BridgeName}_{tmp}";
        BridgeQuery = sq;
    }

    private SelectQuery InjectNotSyncCondition(SelectQuery sq)
    {
        var alias = "m";

        var d = sq.FromClause;
        sq.FromClause.LeftJoin(MapTableName).As(alias).On(x =>
        {
            Datasource.KeyColumnsConfig.ForEach(y => x.Add().Equal(y.Key));
        });
        sq.Where.Add().Column(alias, Datasource.KeyColumnsConfig.First().Key).IsNull();
        return sq;
    }

    private int CreateBridgeTable()
    {
        var q = BridgeQuery.ToCreateTableQuery(BridgeName, true);
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

    private long InsertProcessAndGetId(long tranid)
    {
        var id = (new ProcessRepository(Connection) { Logger = Logger }).Insert(tranid, Datasource, MapTableName);
        Logger?.Invoke($"--process_id : {id}");
        return id;
    }

    private int InsertSync(long procid)
    {
        /*
         * insert into sync (destination_id, process_id)
         * select 
         *     destination_id
         *     , :process_id as process_id
         * from tmp bridge
         */
        var sq = BuildSelectSequenceFromBridge();
        sq.Select.Add().Value(":process_id").As("kms_process_id").AddParameter(":process_id", procid);

        var q = sq.ToInsertQuery(SyncTableName, new());
        return ExecuteQuery(q);
    }

    private int InsertExt(long procid)
    {
        if (BaseDatasource == null) throw new InvalidProgramException();
        var tbl = ExtTableName;
        if (tbl == null) return 0;

        /*
         * insert into ext (base_id, table_name, id)
         * select 
         *     base_id
         *     , table_name
         *     , id
         * from tmp bridge
         */
        var alias = "bridge";

        var sq = new SelectQuery();
        sq.From(BridgeName).As(alias);

        var seq = BaseDatasource.Destination.SequenceConfig;
        var bridge = sq.FromClause;
        //select
        sq.Select.Add().Column(bridge, $"base_{seq.Column}").As(seq.Column);
        sq.Select.Add().Value(":dest_id").As("destination_id").AddParameter(":dest_id", Destination.DestinationId);
        sq.Select.Add().Value(":table_name").As("extension_table_name").AddParameter(":table_name", Destination.TableFulleName);
        sq.Select.Add().Column(bridge, Destination.SequenceConfig.Column).As("id");

        var q = sq.ToInsertQuery(tbl, new());
        return ExecuteQuery(q);
    }

    private int InsertKeyMap()
    {
        /*
         * insert into map (destination_id, datasource_id)
         * select 
         *     destination_id
         *     , datasource_id
         * from tmp bridge
         */
        var sq = BuildSelectSequenceFromBridge();
        var t = sq.FromClause;

        //select
        Datasource.KeyColumnsConfig.ForEach(x => sq.Select(t, x.Key));

        var q = sq.ToInsertQuery(MapTableName, new());
        return ExecuteQuery(q);
    }

    private int InsertDestination()
    {
        /*
         * insert into destination (destination_id, value)
         * select 
         *     destination_id
         *     , value
         * from tmp bridge
         */
        var sq = BuildSelectSequenceFromBridge();
        var bridge = sq.FromClause;

        //select
        var cols = BridgeQuery.GetSelectItems().Select(x => x.ColumnName).ToList();
        Destination.GetInsertColumns().Where(x => cols.Contains(x)).ToList().ForEach(x => sq.Select(bridge, x));

        var q = sq.ToInsertQuery(Destination.TableName, new());
        return ExecuteQuery(q);
    }
}