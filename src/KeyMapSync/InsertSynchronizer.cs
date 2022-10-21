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
    //internal InsertSynchronizer(OffsetSynchronizer owner, Datasource datasource, Action<SelectQuery> injector)
    //{
    //    Connection = owner.Connection;
    //    Datasource = datasource;
    //    Timeout = owner.Timeout;
    //    DestinationResolver = owner.DestinationResolver;
    //    Injector = injector;

    //    Datasource.Destination = DestinationResolver(Datasource.DestinationName);

    //    var tmp = BridgeNameBuilder.GetName(datasource.TableName).Substring(0, 4);
    //    BridgeName = $"{owner.BridgeName}_{tmp}";
    //}

    private InsertSynchronizer(InsertSynchronizer owner, Datasource datasource, Action<SelectQuery, string> injector)
    {
        Connection = owner.Connection;
        SystemConfig = owner.SystemConfig;
        Datasource = datasource;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(String.Concat(Destination.TableFulleName, '_', datasource.DatasourceName)).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}";

        IsRoot = false;

        BridgeQuery = BuildSelectBridgeQuery();
    }

    public InsertSynchronizer(SystemConfig config, IDbConnection connection, Datasource datasource, Action<SelectQuery, string>? injector = null)
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

    private Action<SelectQuery, string>? Injector { get; init; }

    private Datasource Datasource { get; init; }

    private string BridgeName { get; set; }

    private SelectQuery BridgeQuery { get; set; }

    private Destination Destination => Datasource.Destination;

    private KeyMapConfig KeyMapConfig => SystemConfig.KeyMapConfig;

    private string MapTableName => Datasource.GetKeymapTableName(KeyMapConfig);

    private SyncConfig SyncConfig => SystemConfig.SyncConfig;

    private string SyncTableName => Destination.GetSyncTableName(SyncConfig);

    private CommandConfig CommandConfig => SystemConfig.CommandConfig;

    private SelectQuery BuildSelectQueryFromBridgeSelectSequence()
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

    public Result Insert()
    {
        //kms_transaction start
        var rep = new TransactionRepository(Connection) { Logger = Logger };
        var tranid = rep.Insert(Datasource, Argument);
        Logger?.Invoke($"--transaction_id : {tranid}");

        //main
        var result = Insert(tranid);

        //kms_transaction end
        var text = JsonSerializer.ToJsonString(result, StandardResolver.ExcludeNull);
        rep.Update(tranid, text);

        return result;
    }

    internal Result Insert(long tranid)
    {
        Logger?.Invoke($"--insert {Destination.TableFulleName} <- {Datasource.DatasourceName}");

        var result = new Result();

        var cnt = CreateBridgeTable();
        if (cnt == 0) return result;

        //inject header insert
        var hs = BuildHeaderSyncronizer();
        if (hs != null)
        {
            result.Add(hs.Insert(tranid));

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

        //nest
        Datasource.Extensions.ForEach(nestdatasource =>
        {
            //replace root table injector
            Action<SelectQuery, string> replaceRootTable = (q, _) => q.FromClause.TableName = BridgeName;

            var s = new InsertSynchronizer(this, nestdatasource, replaceRootTable) { Logger = Logger };
            result.Add(s.Insert(tranid));
        });
        return result;
    }

    private InsertSynchronizer? BuildHeaderSyncronizer()
    {
        var h = Destination.HeaderDestination;
        if (h == null) return null;

        //sync header
        Action<SelectQuery, string> replaceRootTable = (q, _) => q.FromClause.TableName = BridgeName;
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
        var cols = sq.Select.GetColumnNames();

        //inject from custom function
        if (Injector != null) Injector(sq, Argument);

        sq = sq.PushToCommonTable(alias);

        //from
        var d = sq.From(alias);

        //select
        var seq = Destination.SequenceConfig;
        sq.Select(seq.Command).As(seq.Column);
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
        var sq = BuildSelectQueryFromBridgeSelectSequence();
        sq.Select.Add().Value(":process_id").As("kms_process_id").AddParameter(":process_id", procid);

        var q = sq.ToInsertQuery(SyncTableName, new());
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
        var sq = BuildSelectQueryFromBridgeSelectSequence();
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
        var sq = BuildSelectQueryFromBridgeSelectSequence();
        var bridge = sq.FromClause;

        //select
        var cols = BridgeQuery.GetSelectItems().Select(x => x.ColumnName).ToList();
        Destination.GetInsertColumns().Where(x => cols.Contains(x)).ToList().ForEach(x => sq.Select(bridge, x));

        var q = sq.ToInsertQuery(Destination.TableName, new());
        return ExecuteQuery(q);
    }
}