using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Postgres;

public class IntegrationSaleDetail
{
    public static Destination GetDestination(IDbConnection cn)
    {
        var dbms = new DBMS.Postgres();
        var c = dbms.GetDestination(cn, "keymapsync", "public", "integration_sale_detail");
        c.OffsetConfig = new()
        {
            SignInversionColumns = new() { "quantity", "price" },
        };
        return c;
    }
}

