using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using KeyMapSync.Transform;

namespace KeyMapSync;

public class Synchronizer
{
    public Synchronizer(IDBMS dbms)
    {
        Dbms = dbms;
    }

    public IDBMS Dbms { get; }

    public event EventHandler<SqlEventArgs>? BeforeSqlExecute = null;

    public event EventHandler<SqlResultArgs>? AfterSqlExecute = null;

    private SyncEventArgs? SyncEvent { get; set; } = null;

    public int Insert(IDbConnection cn, Datasource ds, IFilter? filter = null)
    {
        SyncEvent = new SyncEventArgs("Insert");

        //create bridge instance.
        var root = new Abutment(ds);
        var bridge = new AdditionalPier(root);
        if (filter != null) bridge.AddFilter(filter);

        return cn.Transaction(trn => Insert(cn, trn, bridge));
    }

    public int Insert(IDbConnection cn, IDbTransaction trn, IPier bridge)
    {
        var ds = bridge.GetDatasource();

        CreateSystemTable(cn, ds);

        var cnt = CreateTemporaryTable(cn, bridge);
        if (cnt == 0) return 0;

        if (InsertDestination(cn, ds) != cnt) throw new InvalidOperationException();
        InsertGroupDestinations(cn, ds);

        var kmconfig = ds.Destination.KeyMapConfig;
        if (kmconfig != null && InsertKeyMap(cn, ds, kmconfig) != cnt) throw new InvalidOperationException();

        var vconfig = ds.Destination.VersioningConfig;
        if (vconfig != null && InsertSyncAndVersion(cn, ds, vconfig) != cnt) throw new InvalidOperationException();

        //nest
        ds.Extensions.ForEach(x =>
        {
            var abutment = new Abutment(x);
            var pier = new ExtensionAdditionalPier(abutment);
            Insert(cn, trn, pier);
        });

        return cnt;
    }

    public int Offset(IDbConnection cn, Datasource ds, IFilter validateFilter, IFilter? filter = null)
    {
        SyncEvent = new SyncEventArgs("Offset");

        CreateSystemTable(cn, ds);

        var kmconfig = ds.Destination.KeyMapConfig;
        if (kmconfig == null) throw new NotSupportedException($"keymap is not supported.(table:{ds.Destination.DestinationTableName})");
        var offsetconfig = kmconfig.OffsetConfig;
        if (offsetconfig == null) throw new NotSupportedException($"offset is not supported.(table:{ds.Destination.DestinationTableName})");

        //create bridge instance.
        var root = new Abutment(ds);
        var pier = new ExpectPier(root, validateFilter);
        if (filter != null) pier.AddFilter(filter);
        var bridge = new ChangedPier(pier);

        var offsetCount = CreateTemporaryTable(cn, bridge);
        if (offsetCount == 0) return 0;

        cn.Transaction(_ =>
        {
            var vconfig = ds.Destination.VersioningConfig;

            // offset
            var offsetPrefix = offsetconfig.OffsetColumnPrefix;
            if (ReverseInsertDestination(cn, bridge) != offsetCount) throw new InvalidOperationException();
            if (RemoveKeyMap(cn, bridge) != offsetCount) throw new InvalidOperationException();
            if (InsertOffsetKeyMap(cn, bridge) != offsetCount) throw new InvalidOperationException();
            if (vconfig != null && InsertSyncAndVersion(cn, ds, vconfig, offsetPrefix) != offsetCount) throw new InvalidOperationException();

            // renewwal
            var renewalPrefix = offsetconfig.RenewalColumnPrefix;
            var renewalCount = InsertDestination(cn, ds, renewalPrefix);
            if (renewalCount != 0 && InsertKeyMap(cn, ds, kmconfig, renewalPrefix) != renewalCount) throw new InvalidOperationException();
            if (vconfig != null && InsertSyncAndVersion(cn, ds, vconfig) != renewalCount) throw new InvalidOperationException();

            //InsertExtension(cn, bridge, prefix);
        });

        return offsetCount;
    }

    public void CreateSystemTable(IDbConnection cn, Datasource ds)
    {
        ds.ToSystemDbTables().ForEach(x => cn.Execute(Dbms.ToCreateTableSql(x)));
    }

    private int ExecuteSql(IDbConnection cn, (string commandText, IDictionary<string, object>? parameter) command, string caption, string? counterSql = null)
    {
        var e = OnBeforeSqlExecute(caption, command);
        var cnt = cn.Execute(command);
        if (counterSql != null) cnt = cn.ExecuteScalar<int>(counterSql);
        if (e != null) OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    private int ExecuteSql(IDbConnection cn, SqlCommand command, string caption, string? counterSql = null)
    {
        var e = OnBeforeSqlExecute(caption, command);
        var cnt = cn.Execute(command);
        if (counterSql != null) cnt = cn.ExecuteScalar<int>(counterSql);
        if (e != null) OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    private int CreateTemporaryView(IDbConnection cn, IAbutment root)
    {
        var sql = root.ToTemporaryViewDdl();
        var cnt = ExecuteSql(cn, (sql, null), "CreateTemporaryTable");

        return cnt;
    }

    public int CreateTemporaryTable(IDbConnection cn, IPier bridge)
    {
        CreateTemporaryView(cn, bridge.GetAbutment());

        var cmd = bridge.ToCreateTableCommand();
        var counterSql = $"select count(*) from {bridge.GetDatasource().BridgeName};";
        var cnt = ExecuteSql(cn, cmd, "CreateTemporaryTable", counterSql);

        return cnt;
    }

    public int InsertDestination(IDbConnection cn, Datasource d, string? sequencePrefix = null)
    {
        var cmd = d.Destination.ToInsertCommand(d, sequencePrefix);
        var cnt = ExecuteSql(cn, cmd, "InsertDestination");

        return cnt;
    }

    public int InsertGroupDestinations(IDbConnection cn, Datasource d)
    {
        var cnt = 0;
        foreach (var cmd in d.Destination.Groups.Select(x => x.ToInsertCommand(d)))
        {
            cnt += ExecuteSql(cn, cmd, "InsertDestination(Group)");
        }
        return cnt;
    }

    public int ReverseInsertDestination(IDbConnection cn, IBridge bridge)
    {
        var cmd = bridge.ToReverseInsertDestinationCommand();
        var cnt = ExecuteSql(cn, cmd, "ReverseInsertDestination");

        return cnt;
    }

    public int InsertKeyMap(IDbConnection cn, Datasource d, KeyMapConfig config, string? sequencePrefix = null)
    {
        var cmd = config.ToInsertCommand(d, sequencePrefix);
        var cnt = ExecuteSql(cn, cmd, "InsertKeyMap");

        return cnt;
    }

    public int InsertOffsetKeyMap(IDbConnection cn, IBridge bridge, string? prefix = null)
    {
        var cmd = bridge.ToInsertOffsetCommand();
        var cnt = ExecuteSql(cn, cmd, "InsertOffsetKeyMap");

        return cnt;
    }

    public int RemoveKeyMap(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToRemoveKeyMapCommand();
        var cnt = ExecuteSql(cn, sql, "InsertOffsetKeyMap");

        return cnt;
    }

    public int InsertSyncAndVersion(IDbConnection cn, Datasource d, VersioningConfig config, string? prefix = null)
    {
        var cnt = InsertSync(cn, d, config, prefix);
        if (prefix == null)
        {
            var cnt2 = InsertVersion(cn, d, config);
            if (cnt2 != 1) throw new InvalidOperationException();
        }
        return cnt;
    }

    private int InsertSync(IDbConnection cn, Datasource d, VersioningConfig config, string? prefix = null)
    {
        var cmd = config.SyncConfig.ToInsertCommand(d, prefix);
        var cnt = ExecuteSql(cn, cmd, "InsertSync");

        return cnt;
    }

    private int InsertVersion(IDbConnection cn, Datasource d, VersioningConfig config)
    {
        var cmd = config.VersionConfig.ToInsertCommand(d);
        var cnt = ExecuteSql(cn, cmd, "InsertVersion");

        return cnt;
    }

    //public void InsertExtension(IDbConnection cn, IBridge bridge)
    //{
    //    var sqls = bridge.ToExtensionSqls();
    //    foreach (var sql in sqls) ExecuteSql(cn, (sql, null), "InsertExtension");
    //}

    private SqlEventArgs? OnBeforeSqlExecute(string name, (string sql, object? prm) command)
    {
        var handler = BeforeSqlExecute;

        if (SyncEvent == null || handler == null) return null;

        var e = new SqlEventArgs(SyncEvent, name, command.sql, command.prm);
        handler(this, e);
        return e;
    }

    private SqlEventArgs? OnBeforeSqlExecute(string name, SqlCommand command)
    {
        var handler = BeforeSqlExecute;

        if (SyncEvent == null || handler == null) return null;

        var e = new SqlEventArgs(SyncEvent, name, command.CommandText, command.Parameters);
        handler(this, e);
        return e;
    }

    private void OnAfterSqlExecute(SqlEventArgs owner, int count)
    {
        var handler = AfterSqlExecute;

        if (owner == null || handler == null) return;

        var e = new SqlResultArgs(owner, count);
        handler(this, e);
    }
}
