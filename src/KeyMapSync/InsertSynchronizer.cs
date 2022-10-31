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
    internal InsertSynchronizer(OffsetSynchronizer owner, Datasource datasource, string temporarySufix = "", bool isRoot = false)
    {
        Connection = owner.Connection;
        SystemConfig = owner.SystemConfig;
        Datasource = datasource;
        Injector = null;

        var tmp = BridgeNameBuilder.GetName(datasource.TableName).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}{temporarySufix}";

        IsRoot = isRoot;

        BridgeQuery = InsertQueryBuilder.BuildSelectValueFromDatasource(datasource, SystemConfig, IsRoot, null);
    }

    internal InsertSynchronizer(RenewSynchronizer owner, Datasource datasource, string temporarySufix = "", bool isRoot = false)
    {
        Connection = owner.Connection;
        SystemConfig = owner.SystemConfig;
        Datasource = datasource;
        Injector = null;

        var tmp = BridgeNameBuilder.GetName(datasource.TableName).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}{temporarySufix}";

        IsRoot = isRoot;

        BridgeQuery = InsertQueryBuilder.BuildSelectValueFromDatasource(datasource, SystemConfig, IsRoot, null);
    }

    private InsertSynchronizer(InsertSynchronizer owner, Datasource datasource, Action<SelectQuery, Datasource> injector)
    {
        Connection = owner.Connection;
        SystemConfig = owner.SystemConfig;
        Datasource = datasource;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(String.Concat(Destination.TableFulleName, '_', datasource.DatasourceName)).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}";

        IsRoot = false;

        BridgeQuery = InsertQueryBuilder.BuildSelectValueFromDatasource(datasource, SystemConfig, IsRoot, injector);
    }

    public InsertSynchronizer(SystemConfig config, IDbConnection connection, Datasource datasource, Action<SelectQuery, Datasource>? injector = null)
    {
        SystemConfig = config;
        Connection = connection;
        Datasource = datasource;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(String.Concat(Destination.TableFulleName, '_', datasource.DatasourceName)).Substring(0, 4);
        BridgeName = $"_{tmp}";

        IsRoot = true;

        BridgeQuery = InsertQueryBuilder.BuildSelectValueFromDatasource(datasource, SystemConfig, IsRoot, injector);
    }

    public SystemConfig SystemConfig { get; init; }

    public Action<string>? Logger { get; set; } = null;

    public string Argument { get; set; } = String.Empty;

    private bool IsRoot { get; init; }

    private IDbConnection Connection { get; init; }

    private Action<SelectQuery, Datasource>? Injector { get; init; }

    private Datasource Datasource { get; init; }

    internal string BridgeName { get; set; }

    private SelectQuery BridgeQuery { get; set; }

    private Destination Destination => Datasource.Destination;

    private KeyMapConfig KeyMapConfig => SystemConfig.KeyMapConfig;

    private string MapTableName => Datasource.GetKeymapTableName(KeyMapConfig);

    private SyncConfig SyncConfig => SystemConfig.SyncConfig;

    private string SyncTableName => Destination.GetSyncTableName(SyncConfig);

    private ExtendConfig ExtendConfig => SystemConfig.ExtendConfig;

    private string ExtTableName => Datasource.GetRootDatasource().Destination.GetExtendTableName(ExtendConfig);

    private CommandConfig CommandConfig => SystemConfig.CommandConfig;

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
        if (Destination.IsHeader == false && !Datasource.IsRoot && ExtTableName != null)
        {
            if (InsertExt(procid) != cnt) throw new InvalidOperationException();
            result.Add(new Result() { Table = ExtTableName, Count = cnt });
        }

        //nest
        Datasource.Extensions.ForEach(ds =>
        {
            //replace root table injector
            Action<SelectQuery, Datasource> replaceRootTable = (q, _) => q.FromClause.TableName = BridgeName;

            ds.BaseDatasource = Datasource;

            var s = new InsertSynchronizer(this, ds, replaceRootTable) { Logger = Logger };
            result.Add(s.Execute(tranid));
        });
        return result;
    }

    private InsertSynchronizer? BuildHeaderSyncronizer()
    {
        var h = Destination.HeaderDestination;
        if (h == null) return null;

        //sync header
        Action<SelectQuery, Datasource> replaceRootTable = (q, _) => q.FromClause.TableName = BridgeName;
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

    private int CreateBridgeTable()
    {
        var q = BridgeQuery.ToCreateTableQuery(BridgeName, true);
        ExecuteQuery(q);

        q = InsertQueryBuilder.BuildCountQuery(BridgeName);
        return ExecuteScalar<int>(q);
    }

    private long InsertProcessAndGetId(long tranid)
    {
        var id = (new ProcessRepository(Connection) { Logger = Logger }).Insert(tranid, Datasource, MapTableName);
        Logger?.Invoke($"--process_id : {id}");
        return id;
    }

    private int InsertSync(long procid)
    {
        var q = InsertQueryBuilder.BuildeInsertSyncFromBridge(procid, Datasource, BridgeName, SystemConfig);
        return ExecuteQuery(q);
    }

    private int InsertExt(long procid)
    {
        var q = InsertQueryBuilder.BuildInsertExtFromBridge(Datasource, BridgeName, SystemConfig);
        return ExecuteQuery(q);
    }

    private int InsertKeyMap()
    {
        var q = InsertQueryBuilder.BuildInsertKeymapFromBridge(Datasource, BridgeName, SystemConfig);
        return ExecuteQuery(q);
    }

    private int InsertDestination()
    {
        var cols = BridgeQuery.GetSelectItems().Select(x => x.ColumnName).ToList();
        var q = InsertQueryBuilder.BuildInsertDestinationFromBridge(Datasource, BridgeName, cols);
        return ExecuteQuery(q);
    }
}