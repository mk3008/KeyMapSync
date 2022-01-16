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
    public IDBMS Dbms { get; set; }

    public event EventHandler<SqlEventArgs> BeforeSqlExecute;

    public event EventHandler<SqlResultArgs> AfterSqlExecute;

    public SyncEventArgs SyncEvent { get; set; }

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

        using (var trn = cn.BeginTransaction())
        {
            CreateSystemTable(cn, ds);

            var root = new Abutment(ds, bridgeName);
            var pier = new ExpectPier(root, validateFilter);
            pier.AddFilter(filter);
            var bridge = new ChangedPier(pier);

            var cnt = CreateTemporaryTable(cn, bridge);
            if (cnt == 0) return 0;

            //if (InsertReverseDestination(cn, bridge, "renewal") != cnt) throw new InvalidOperationException();
            if (RemoveKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();
            //if (InsertOffsetKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();

            //TODO:count check!
            if (InsertDestination(cn, bridge, "renewal") != cnt) throw new InvalidOperationException();
            if (InsertKeyMap(cn, bridge, "renewal") != cnt) throw new InvalidOperationException();
            if (InsertSync(cn, bridge, "renewal") != cnt) throw new InvalidOperationException();
            if (InsertVersion(cn, bridge) != 1) throw new InvalidOperationException();

            //InsertExtension(cn, bridge);

            trn.Commit();

            return cnt;
        }
    }

    public void CreateSystemTable(IDbConnection cn, Datasource ds)
    {
        cn.Execute(Dbms.ToKeyMapDDL(ds));
        cn.Execute(Dbms.ToSyncDDL(ds));
        cn.Execute(Dbms.ToVersionDDL(ds));
        cn.Execute(Dbms.ToOffsetDDL(ds));
    }

    private int CreateTemporaryView(IDbConnection cn, IAbutment root)
    {
        var sql = root.ToTemporaryViewDdl();

        var e = OnBeforeSqlExecute("CreateTemporaryView", sql, null);
        var cnt = cn.Execute(sql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int CreateTemporaryTable(IDbConnection cn, IPier bridge, bool isTemporary = true)
    {
        CreateTemporaryView(cn, bridge.GetAbutment());

        var cmd = bridge.ToCreateTableCommand(isTemporary);

        var e = OnBeforeSqlExecute("CreateTemporaryTable", cmd);
        cn.Execute(cmd);
        var cntSql = $"select count(*) from {bridge.GetBridgeName()};";
        var cnt = cn.ExecuteScalar<int>(cntSql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int InsertDestination(IDbConnection cn, IBridge bridge, string prefix = null)
    {
        var cmd = bridge.ToInsertDestinationCommand(prefix);

        var e = OnBeforeSqlExecute("InsertDestination", cmd);
        var cnt = cn.Execute(cmd);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int InsertKeyMap(IDbConnection cn, IBridge bridge, string prefix = null)
    {
        var cmd = bridge.ToInsertKeyMapCommand(prefix);

        var e = OnBeforeSqlExecute("InsertKeyMap", cmd);
        var cnt = cn.Execute(cmd);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int RemoveKeyMap(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToRemoveKeyMapSql();

        var e = OnBeforeSqlExecute("RemoveKeyMap", sql, null);
        var cnt = cn.Execute(sql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int InsertSync(IDbConnection cn, IBridge bridge, string prefix = null)
    {
        var cmd = bridge.ToInsertSyncCommand(prefix);

        var e = OnBeforeSqlExecute("InsertSync", cmd);
        var cnt = cn.Execute(cmd);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int InsertVersion(IDbConnection cn, IBridge bridge)
    {
        var cmd = bridge.ToInsertVersionCommand();

        var e = OnBeforeSqlExecute("InsertVersion", cmd);
        var cnt = cn.Execute(cmd);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public void InsertExtension(IDbConnection cn, IBridge bridge)
    {
        var sqls = bridge.ToExtensionSqls();
        foreach (var sql in sqls)
        {
            var e = OnBeforeSqlExecute("InsertExtension", sql, null);
            var cnt = cn.Execute(sql);
            OnAfterSqlExecute(e, cnt);
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
