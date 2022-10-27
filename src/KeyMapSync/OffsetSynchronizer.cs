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
    public OffsetSynchronizer(SystemConfig config, IDbConnection connection, IDBMS dbms, Datasource datasource, Action<SelectQuery, Datasource>? injector = null)
    {
        SystemConfig = config;
        Connection = connection;
        Datasource = datasource;
        Injector = injector;
        Dbms = dbms;

        var tmp = BridgeNameBuilder.GetName(String.Concat(Destination.TableFulleName, '_', datasource.DatasourceName)).Substring(0, 4);
        BridgeName = $"_{tmp}";

        IsRoot = true;

        BridgeQuery = OffsetQueryBuilder.BuildSelectBridgeQuery(Datasource, SystemConfig, Injector);
    }

    public SystemConfig SystemConfig { get; init; }

    public Action<string>? Logger { get; set; } = null;

    public string Argument { get; set; } = String.Empty;

    private IDBMS Dbms { get; init; }

    private bool IsRoot { get; init; }

    internal IDbConnection Connection { get; init; }

    private Action<SelectQuery, Datasource>? Injector { get; init; }

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

    private ExtendConfig ExtendConfig => SystemConfig.ExtendConfig;

    private string ExtTableName => Destination.GetExtendTableName(ExtendConfig);

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

        return result;
    }


    private int CreateBridgeTable()
    {
        var q = BridgeQuery.ToCreateTableQuery(BridgeName, true);
        ExecuteQuery(q);

        q = InsertQueryBuilder.BuildCountQuery(BridgeName);
        return ExecuteScalar<int>(q);
    }

    private int ExecuteQuery(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var cnt = Connection.Execute(q, commandTimeout: CommandConfig.Timeout);
        Logger?.Invoke($"count : {cnt}");

        return cnt;
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
        var offset = Destination.GetOffsetTableName(OffsetConfig);

        var q = OffsetQueryBuilder.BuildInsertOffsetMap(Datasource, BridgeName, SystemConfig);
        var cnt = ExecuteQuery(q);
        return new Result() { Table = offset, Count = cnt };
    }

    private Result DeleteKeyMap()
    {
        var map = Datasource.GetKeymapTableName(KeyMapConfig);

        var q = OffsetQueryBuilder.BuildDeleteKeyMap(Datasource, BridgeName, SystemConfig);
        var cnt = ExecuteQuery(q);
        return new Result() { Table = map, Count = cnt, Command = "delete" };
    }

    private Result InsertOffset(long tranid)
    {
        //virtual datasource
        var ds = OffsetQueryBuilder.BuildOffsetDatasrouce(Datasource, BridgeName, SystemConfig);
        var s = new InsertSynchronizer(this, ds) { Logger = Logger };
        var r = s.Execute(tranid);
        r.Caption = "insert offset";

        var extids = GetOffsetExtentDestinations();
        var rep = new DestinationRepository(Dbms, Connection);
        var cnt = 0;
        extids.ForEach(x =>
        {
            var ext = rep.FindById(x);
            var d = OffsetQueryBuilder.BuildOffsetExtensionDatasrouce(Datasource, ext, BridgeName, SystemConfig);
            var s = new InsertSynchronizer(this, d, $"E{cnt}") { Logger = Logger };
            r.Add(s.Execute(tranid));
            cnt++;
        });
        return r;
    }

    private List<long> GetOffsetExtentDestinations()
    {
        var q = OffsetQueryBuilder.BuildSelectOffsetDestinationIdsFromBridge(Datasource, BridgeName, SystemConfig);
        return Connection.Query<long>(q).ToList();
    }

    private string GetOffsetExtensionQuery(Destination extension)
    {
        var q = OffsetQueryBuilder.BuildSelectOffsetExtensionFromBridge(Datasource, BridgeName, extension, SystemConfig);
        return q.CommandText;
    }

    private string GetSelectOffsetDatasourceQuery()
    {
        var q = OffsetQueryBuilder.BuildSelectOffsetDatasourceFromBridge(Datasource, BridgeName, SystemConfig);
        return q.CommandText;
    }

    private Result InsertRenew(long tranid)
    {
        //virtual datasource
        var ds = OffsetQueryBuilder.BuildRenewDatasource(Datasource, BridgeName, SystemConfig);
        var s = new InsertSynchronizer(this, ds, "r", true) { Logger = Logger };
        var r = s.Execute(tranid);
        r.Caption = "insert renew";
        return r;
    }
}