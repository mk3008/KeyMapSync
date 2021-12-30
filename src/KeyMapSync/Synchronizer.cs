using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using KeyMapSync.Entity;
using KeyMapSync.Transform;

namespace KeyMapSync;

public class Synchronizer
{
    public IDBMS Dbms { get; set; }

    public void CreateSystemTable(IDbConnection cn, Datasource ds)
    {
        cn.Execute(Dbms.ToKeyMapDDL(ds));
        cn.Execute(Dbms.ToSyncDDL(ds));
        cn.Execute(Dbms.ToVersionDDL(ds));
        cn.Execute(Dbms.ToOffsetDDL(ds));
    }

    public void CreateTemporaryTable(IDbConnection cn, IBridge bridge, bool isTemporary = true )
    {
        var sql = bridge.ToTemporaryDdl(isTemporary);
        var prm = bridge.ToTemporaryParameter();
        cn.Execute(sql, prm);
    }

    public void InsertDestination(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToDestinationSql();
        cn.Execute(sql);
    }

    public void InsertKeyMap(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToKeyMapSql();
        cn.Execute(sql);
    }

    public void InsertSync(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToSyncSql();
        cn.Execute(sql);
    }

    public void InsertVersion(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToVersionSql();
        var prm = bridge.ToVersionParameter();
        cn.Execute(sql, prm);
    }

    public void InsertExtension(IDbConnection cn, IBridge bridge)
    {
        var sqls = bridge.ToExtensionSqls();
        foreach (var sql in sqls)
        {
            cn.Execute(sql);
        }   
    }
}
