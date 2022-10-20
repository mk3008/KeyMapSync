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

        var tmp = BridgeNameBuilder.GetName(datasource.TableFulleName).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}";

        IsRoot = false;
    }

    public InsertSynchronizer(SystemConfig config, IDbConnection connection, Datasource datasource, Action<SelectQuery, string>? injector = null)
    {
        SystemConfig = config;
        Connection = connection;
        Datasource = datasource;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(datasource.TableFulleName).Substring(0, 4);
        BridgeName = $"_{tmp}";

        IsRoot = true;
    }

    public SystemConfig SystemConfig { get; init; }

    public Action<string>? Logger { get; set; } = null;

    public string Argument { get; set; } = String.Empty;

    private bool IsRoot { get; init; }

    private IDbConnection Connection { get; init; }

    private Action<SelectQuery, string>? Injector { get; init; }

    private Datasource Datasource { get; init; }

    private string BridgeName { get; init; }

    private Destination Destination => Datasource.Destination;

    private KeyMapConfig KeyMapConfig => SystemConfig.KeyMapConfig;

    private string MapTableName => Datasource.GetKeymapTableName(KeyMapConfig);

    private SyncConfig SyncConfig => SystemConfig.SyncConfig;

    private string SyncTableName => Destination.GetSyncTableName(SyncConfig);

    private CommandConfig CommandConfig => SystemConfig.CommandConfig;

    private SelectQuery CreateSelectFromBridgeQuery()
    {
        var alias = "bridge";

        var sq = new SelectQuery();
        sq.From(BridgeName).As(alias);

        return sq;
    }

    private SelectQuery CreateSelectFromBridgeQueryAsAdditional()
    {
        var sq = CreateSelectFromBridgeQuery();
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
        Logger?.Invoke($"--insert {Destination.TableFulleName} <- {Datasource.TableFulleName}");
        var sq = BuildSelectBridgeQuery();
        var result = Insert(sq, tranid);

        //kms_transaction end
        var text = JsonSerializer.ToJsonString(result, StandardResolver.ExcludeNull);
        rep.Update(tranid, text);

        return result;
    }

    internal Result Insert(int tranid)
    {
        Logger?.Invoke($"--extend insert {Destination.TableFulleName} <- {Datasource.TableFulleName}");

        var sq = BuildSelectBridgeQuery();

        return Insert(sq, tranid);
    }

    private Result Insert(SelectQuery bridgequery, int tranid)
    {
        var result = new Result();

        var cnt = CreateBridgeTable(bridgequery);
        if (cnt == 0) return result;

        //process
        var procid = GetNewProcessId(tranid);
        result.Add(new Result() { Table = "kms_processes", Count = cnt });

        //destination
        if (InsertDestination(bridgequery) != cnt) throw new InvalidOperationException();
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
        sq = InjectNotSyncCondition(sq);

        return sq;
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

    private int GetNewProcessId(int tranid)
    {
        var id = (new ProcessRepository(Connection) { Logger = Logger }).Insert(tranid, Datasource, MapTableName);
        Logger?.Invoke($"--process_id : {id}");
        return id;
    }

    private int InsertSync(int procid)
    {
        /*
         * insert into sync (destination_id, process_id)
         * select 
         *     destination_id
         *     , :process_id as process_id
         * from tmp bridge
         */
        var sq = CreateSelectFromBridgeQueryAsAdditional();
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
        var sq = CreateSelectFromBridgeQueryAsAdditional();
        var t = sq.FromClause;

        //select
        Datasource.KeyColumnsConfig.ForEach(x => sq.Select(t, x.Key));

        var q = sq.ToInsertQuery(MapTableName, new());
        return ExecuteQuery(q);
    }

    private int InsertDestination(SelectQuery bridgequery)
    {
        /*
         * insert into destination (destination_id, value)
         * select 
         *     destination_id
         *     , value
         * from tmp bridge
         */
        var sq = CreateSelectFromBridgeQueryAsAdditional();
        var bridge = sq.FromClause;

        //select
        var cols = bridgequery.Select.GetColumnNames().ToList();
        Destination.GetInsertColumns().Where(x => cols.Contains(x)).ToList().ForEach(x => sq.Select(bridge, x));

        var q = sq.ToInsertQuery(Destination.TableName, new());
        return ExecuteQuery(q);
    }
}