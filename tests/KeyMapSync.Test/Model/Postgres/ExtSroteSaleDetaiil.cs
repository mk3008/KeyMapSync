using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Postgres;

public class ExtSroteSaleDetaiil
{
    public static Destination GetDestination(IDbConnection cn)
    {
        var dbms = new DBMS.Postgres();
        var c = dbms.GetDestination(cn, "public", "integration_sale_detail_ext_store_sale_detail");
        return c;
    }
}

