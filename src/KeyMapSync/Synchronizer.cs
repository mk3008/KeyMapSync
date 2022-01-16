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

    public event EventHandler<SqlEventArgs> BeforeSqlExecute;

    public event EventHandler<SqlResultArgs> AfterSqlExecute;

    private SyncEventArgs SyncEvent { get; set; }

    public int Insert(IDbConnection cn, Datasource ds, string bridgeName = null, IFilter filter = null)
    {
        SyncEvent = new SyncEventArgs("Insert");

        CreateSystemTable(cn, ds);
        var root = new Abutment(ds, bridgeName);
        var bridge = new AdditionalPier(root);
        bridge.AddFilter(filter);

        var cnt = CreateTemporaryTable(cn, bridge);
        if (cnt == 0) return 0;

        using (var trn = cn.BeginTransaction())
        {
            if (InsertDestination(cn, bridge) != cnt) throw new InvalidOperationException();
            if (InsertKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();
            if (InsertSync(cn, bridge) != cnt) throw new InvalidOperationException();
            if (InsertVersion(cn, bridge) != 1) throw new InvalidOperationException();

            InsertExtension(cn, bridge);

            trn.Commit();

            return cnt;
        }
    }

    public int Offset(IDbConnection cn, Datasource ds, IFilter validateFilter, string bridgeName = null, IFilter filter = null)
    {
        SyncEvent = new SyncEventArgs("Offset");

        CreateSystemTable(cn, ds);

        var root = new Abutment(ds, bridgeName);
        var pier = new ExpectPier(root, validateFilter);
        pier.AddFilter(filter);
        var bridge = new ChangedPier(pier);

        var offsetCount = CreateTemporaryTable(cn, bridge);
        if (offsetCount == 0) return 0;

        using (var trn = cn.BeginTransaction())
        {
            // offset
            var offsetPrefix = ds.Destination.OffsetColumnPrefix;
            if (ReverseInsertDestination(cn, bridge) != offsetCount) throw new InvalidOperationException();
            if (RemoveKeyMap(cn, bridge) != offsetCount) throw new InvalidOperationException();
            if (InsertOffsetKeyMap(cn, bridge) != offsetCount) throw new InvalidOperationException();
            if (InsertSync(cn, bridge, offsetPrefix) != offsetCount) throw new InvalidOperationException();

            // renewwal
            var renewalPrefix = ds.Destination.RenewalColumnPrefix;
            var renewalCount = InsertDestination(cn, bridge, renewalPrefix);
            if (renewalCount != 0)
            {
                if (InsertKeyMap(cn, bridge, renewalPrefix) != renewalCount) throw new InvalidOperationException();
                if (InsertSync(cn, bridge, renewalPrefix) != renewalCount) throw new InvalidOperationException();
            }
            if (InsertVersion(cn, bridge) != 1) throw new InvalidOperationException();

            //InsertExtension(cn, bridge, prefix);

            trn.Commit();

            return offsetCount;
        }
    }

    public void CreateSystemTable(IDbConnection cn, Datasource ds)
    {
        cn.Execute(Dbms.ToKeyMapDDL(ds));
        cn.Execute(Dbms.ToSyncDDL(ds));
        cn.Execute(Dbms.ToVersionDDL(ds));
        cn.Execute(Dbms.ToOffsetDDL(ds));
    }

    private int ExecuteSql(IDbConnection cn, (string commandText, IDictionary<string, object> parameter) command, string caption, string counterSql = null)
    {
        var e = OnBeforeSqlExecute(caption, command);
        var cnt = cn.Execute(command);
        if (counterSql != null) cnt = cn.ExecuteScalar<int>(counterSql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    private int CreateTemporaryView(IDbConnection cn, IAbutment root)
    {
        var sql = root.ToTemporaryViewDdl();
        var cnt = ExecuteSql(cn, (sql, null), "CreateTemporaryTable");

        return cnt;
    }

    public int CreateTemporaryTable(IDbConnection cn, IPier bridge, bool isTemporary = true)
    {
        CreateTemporaryView(cn, bridge.GetAbutment());

        var cmd = bridge.ToCreateTableCommand(isTemporary);
        var counterSql = $"select count(*) from {bridge.GetBridgeName()};";
        var cnt = ExecuteSql(cn, cmd, "CreateTemporaryTable", counterSql);

        return cnt;
    }

    public int InsertDestination(IDbConnection cn, IBridge bridge, string prefix = null)
    {
        var cmd = bridge.ToInsertDestinationCommand(prefix);
        var cnt = ExecuteSql(cn, cmd, "InsertDestination");

        return cnt;
    }

    public int ReverseInsertDestination(IDbConnection cn, IBridge bridge)
    {
        var cmd = bridge.ToReverseInsertDestinationCommand();
        var cnt = ExecuteSql(cn, cmd, "ReverseInsertDestination");

        return cnt;
    }

    public int InsertKeyMap(IDbConnection cn, IBridge bridge, string prefix = null)
    {
        var cmd = bridge.ToInsertKeyMapCommand(prefix);
        var cnt = ExecuteSql(cn, cmd, "InsertKeyMap");

        return cnt;
    }

    public int InsertOffsetKeyMap(IDbConnection cn, IBridge bridge, string prefix = null)
    {
        var cmd = bridge.ToInsertOffsetKeyMapCommand();
        var cnt = ExecuteSql(cn, cmd, "InsertOffsetKeyMap");

        return cnt;
    }

    public int RemoveKeyMap(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToRemoveKeyMapSql();
        var cnt = ExecuteSql(cn, (sql, null), "InsertOffsetKeyMap");

        return cnt;
    }

    public int InsertSync(IDbConnection cn, IBridge bridge, string prefix = null)
    {
        var cmd = bridge.ToInsertSyncCommand(prefix);
        var cnt = ExecuteSql(cn, cmd, "InsertSync");

        return cnt;
    }

    public int InsertVersion(IDbConnection cn, IBridge bridge)
    {
        var cmd = bridge.ToInsertVersionCommand();
        var cnt = ExecuteSql(cn, cmd, "InsertVersion");

        return cnt;
    }

    public void InsertExtension(IDbConnection cn, IBridge bridge)
    {
        var sqls = bridge.ToExtensionSqls();
        foreach (var sql in sqls)
        {
             ExecuteSql(cn, (sql, null), "InsertExtension");
        }
    }

    private SqlEventArgs OnBeforeSqlExecute(string name, string sql, object prm)
    {
        var handler = BeforeSqlExecute;

        if (SyncEvent == null || handler == null) return null;

        var e = new SqlEventArgs(SyncEvent, name, sql, prm);
        handler(this, e);
        return e;
    }

    private SqlEventArgs OnBeforeSqlExecute(string name, (string sql, object prm) command)
    {
        var handler = BeforeSqlExecute;

        if (SyncEvent == null || handler == null) return null;

        var e = new SqlEventArgs(SyncEvent, name, command.sql, command.prm);
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
