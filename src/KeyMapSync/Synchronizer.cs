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

    public void CreateTemporaryTable(IDbConnection cn, IBridge bridge)
    {
        var sql = bridge.ToSql();
        var prm = bridge.ToParameter();

    }
}
