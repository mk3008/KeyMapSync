using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using KeyMapSync.DBMS;
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

        //create system table, temporary view, temporary table.
        CreateEnvironment(cn, bridge);

        return cn.Transaction(trn => Insert(cn, trn, bridge));
    }

    private void CreateEnvironment(IDbConnection cn, IPier bridge)
    {
        var ds = bridge.GetDatasource();
        CreateSystemTable(cn, ds);
        CreateTemporaryView(cn, bridge.Abutment);
        CreateTemporaryTable(cn, bridge);

        //nest
        ds.Extensions.ForEach(x =>
        {
            var abutment = new Abutment(x);
            var pier = new ExtensionAdditionalPier(abutment);
            CreateEnvironment(cn, pier);
        });
    }

    public int Insert(IDbConnection cn, IDbTransaction trn, IPier bridge)
    {
        var ds = bridge.GetDatasource();

        var cnt = GetCountTemporaryTable(cn, bridge);
        if (cnt == 0) return 0;

        if (InsertDestination(cn, ds) != cnt) throw new InvalidOperationException();
        InsertGroupDestinations(cn, ds);

        var kmconfig = ds.Destination.KeyMapConfig;
        if (kmconfig != null && InsertKeyMap(cn, ds, kmconfig) != cnt) throw new InvalidOperationException();

        var vconfig = ds.Destination.VersioningConfig;
        if (vconfig != null && InsertSyncAndVersion(cn, bridge, vconfig) != cnt) throw new InvalidOperationException();

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

        //create bridge instance.
        var root = new Abutment(ds);
        var pier = new ExpectPier(root, validateFilter);
        if (filter != null) pier.AddFilter(filter);
        var bridge = new ChangedPier(pier);

        //create system table, temporary view, temporary table.
        CreateEnvironment(cn, bridge);

        return cn.Transaction(trn => Offset(cn, trn, bridge));
    }

    public int Offset(IDbConnection cn, IDbTransaction trn, IPier bridge)
    {
        var ds = bridge.GetDatasource();

        var kmconfig = ds.Destination.KeyMapConfig;
        if (kmconfig == null) throw new NotSupportedException($"keymap is not supported.(table:{ds.Destination.DestinationTableName})");
        var offsetconfig = kmconfig.OffsetConfig;
        if (offsetconfig == null) throw new NotSupportedException($"offset is not supported.(table:{ds.Destination.DestinationTableName})");

        var vconfig = ds.Destination.VersioningConfig;

        var cnt = GetCountTemporaryTable(cn, bridge);
        if (cnt == 0) return 0;

        // offset
        var offsetPrefix = offsetconfig.OffsetColumnPrefix;
        if (ReverseInsertDestination(cn, bridge) != cnt) throw new InvalidOperationException();
        if (RemoveKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();
        if (InsertOffsetKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();
        if (vconfig != null && InsertSync(cn, ds, vconfig, offsetPrefix) != cnt) throw new InvalidOperationException();

        // renewwal
        var renewalPrefix = offsetconfig.RenewalColumnPrefix;
        var renewalCount = InsertDestination(cn, ds, renewalPrefix);
        if (renewalCount != 0 && InsertKeyMap(cn, ds, kmconfig, renewalPrefix) != renewalCount) throw new InvalidOperationException();
        if (renewalCount != 0 && vconfig != null && InsertSync(cn, ds, vconfig, renewalPrefix) != renewalCount) throw new InvalidOperationException();

        //version
        if (vconfig != null) InsertVersion(cn, bridge, vconfig);

        //InsertExtension(cn, bridge, prefix);
        //nest
        //ds.Extensions.ForEach(x =>
        //{
        //    var abutment = new Abutment(x);
        //    var pier = new ExtensionAdditionalPier(abutment);
        //    Insert(cn, trn, pier);
        //});

        return cnt;
    }

    public void CreateSystemTable(IDbConnection cn, Datasource ds)
    {
        ds.ToSystemDbTables().ForEach(x => cn.Execute(Dbms.ToCreateTableSql(x)));
    }

    private void CreateTemporaryView(IDbConnection cn, IAbutment root)
    {
        var cmd = root.ToTemporaryViewDdl();
        ExecuteSql(cn, cmd, $"CreateTemporaryView : {root.ViewName}");
    }

    public void CreateTemporaryTable(IDbConnection cn, IPier bridge)
    {
        var table = bridge.GetDatasource().BridgeName;
        var cmd = bridge.ToCreateTableCommand();
        ExecuteSql(cn, cmd, $"CreateTemporaryTable : {table}");
    }

    public int GetCountTemporaryTable(IDbConnection cn, IPier bridge)
    {
        var table = bridge.GetDatasource().BridgeName;
        var cmd = new SqlCommand()
        {
            CommandText = $"select count(*) from {table};"
        };
        var cnt = ExecuteScalarSql(cn, cmd, $"GetCountTemporaryTable : {table}");
        return cnt;
    }

    private int ExecuteSql(IDbConnection cn, SqlCommand command, string caption)
    {
        var e = OnBeforeSqlExecute(caption, command);
        var cnt = cn.Execute(command);
        if (e != null) OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    private int ExecuteScalarSql(IDbConnection cn, SqlCommand command, string caption)
    {
        var e = OnBeforeSqlExecute(caption, command);
        var cnt = cn.ExecuteScalar<int>(command.CommandText);
        if (e != null) OnAfterSqlExecute(e, cnt);

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

    public int InsertSyncAndVersion(IDbConnection cn, IPier pier, VersioningConfig config)
    {
        var d = pier.GetDatasource();
        var cnt = InsertSync(cn, d, config);

        var cnt2 = InsertVersion(cn, pier, config);
        if (cnt2 != 1) throw new InvalidOperationException();

        return cnt;
    }

    private int InsertSync(IDbConnection cn, Datasource d, VersioningConfig config, string? prefix = null)
    {
        var cmd = config.SyncConfig.ToInsertCommand(d, prefix);
        var cnt = ExecuteSql(cn, cmd, "InsertSync");

        return cnt;
    }

    private int InsertVersion(IDbConnection cn, IPier pier, VersioningConfig config)
    {
        var cmd = config.VersionConfig.ToInsertCommand(pier);
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
