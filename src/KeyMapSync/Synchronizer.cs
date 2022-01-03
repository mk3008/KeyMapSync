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

    public int Insert(IDbConnection cn, Datasource ds, IFilter filter = null)
    {
        SyncEvent = new SyncEventArgs("Insert");

        using (var trn = cn.BeginTransaction())
        {
            CreateSystemTable(cn, ds);

            var tmp = $"_kms_tmp_{ds.Name}";
            var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
            var bridge = new Additional() { Owner = root };
            bridge.AddFilter(filter);

            var cnt = CreateTemporaryTable(cn, bridge);
            if (cnt == 0) return 0;

            if (InsertDestination(cn, bridge) != cnt) throw new InvalidOperationException();
            if (InsertKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();
            if (InsertSync(cn, bridge) != cnt) throw new InvalidOperationException();
            if (InsertVersion(cn, bridge) != 1) throw new InvalidOperationException();

            InsertExtension(cn, bridge);

            trn.Commit();

            return cnt;
        }
    }

    public int Offset(IDbConnection cn, Datasource ds, IFilter validateFilter, IFilter filter = null)
    {
        SyncEvent = new SyncEventArgs("Offset");

        using (var trn = cn.BeginTransaction())
        {
            CreateSystemTable(cn, ds);

            var tmp = $"_kms_tmp_{ds.Name}";
            var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
            var work = new ExpectBridge() { Owner = root, };
            work.AddFilter(validateFilter);
            work.AddFilter(filter);
            var bridge = new ChangedBridge() { Owner = work };

            var cnt = CreateTemporaryTable(cn, bridge);
            if (cnt == 0) return 0;

            //if (InsertDestinationAsOffset(cn, bridge) != cnt) throw new InvalidOperationException();
            //if (RemoveKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();
            //if (InsertOffsetKeyMap(cn, bridge) != cnt) throw new InvalidOperationException();
            if (InsertSync(cn, bridge) != cnt) throw new InvalidOperationException();
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

    private int CreateTemporaryView(IDbConnection cn, BridgeRoot root)
    {
        var sql = root.ToTemporaryViewDdl();

        var e = OnBeforeSqlExecute("CreateTemporaryView", sql, null);
        var cnt = cn.Execute(sql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int CreateTemporaryTable(IDbConnection cn, IBridge bridge, bool isTemporary = true)
    {
        CreateTemporaryView(cn, bridge.GetRoot());

        var sql = bridge.ToTemporaryDdl(isTemporary);
        var prm = bridge.ToTemporaryParameter();

        var e = OnBeforeSqlExecute("CreateTemporaryTable", sql, prm);
        cn.Execute(sql, prm);
        var cntSql = $"select count(*) from {bridge.BridgeName};";
        var cnt = cn.ExecuteScalar<int>(cntSql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int InsertDestination(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToDestinationSql();

        var e = OnBeforeSqlExecute("InsertDestination", sql, null);
        var cnt = cn.Execute(sql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int InsertKeyMap(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToKeyMapSql();
        var e = OnBeforeSqlExecute("InsertKeyMap", sql, null);
        var cnt = cn.Execute(sql);
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

    public int InsertSync(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToSyncSql();
        var e = OnBeforeSqlExecute("InsertSync", sql, null);
        var cnt = cn.Execute(sql);
        OnAfterSqlExecute(e, cnt);

        return cnt;
    }

    public int InsertVersion(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToVersionSql();
        var prm = bridge.ToVersionParameter();
        var e = OnBeforeSqlExecute("InsertVersion", sql, prm);
        var cnt = cn.Execute(sql, prm);
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

    private void OnAfterSqlExecute(SqlEventArgs owner, int count)
    {
        var handler = AfterSqlExecute;

        if (owner == null || handler == null) return;

        var e = new SqlResultArgs(owner, count);
        handler(this, e);
    }
}
