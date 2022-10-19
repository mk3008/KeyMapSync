using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Postgres;

internal class DestinationManager
{
    public static Destination GetAccounts(IDbConnection cn)
    {
        var name = "accounts";
        var dbms = new DBMS.Postgres(cn);

        var d = dbms.ResolveDestinationOrDefault(name, () =>
        {
            var c = dbms.CreateDestination(name);
            return c;
        });
        return d;
    }

    public static Destination GetExtDebitAccounts(IDbConnection cn)
    {
        var name = "ext_debit_accounts";
        var dbms = new DBMS.Postgres(cn);

        var d = dbms.ResolveDestinationOrDefault(name, () =>
        {
            var c = dbms.CreateDestination(name);
            c.HasKeyMap = false;
            c.AllowOffset = false;
            return c;
        });
        return d;
    }

    public static Destination GetExtCreditAccounts(IDbConnection cn)
    {
        var name = "ext_credit_accounts";
        var dbms = new DBMS.Postgres(cn);

        var d = dbms.ResolveDestinationOrDefault(name, () =>
        {
            var c = dbms.CreateDestination(name);
            c.HasKeyMap = false;
            c.AllowOffset = false;
            return c;
        });
        return d;
    }

    public static Destination GetExtAccountStaticTrans(IDbConnection cn)
    {
        var name = "ext_account_static_trans";
        var dbms = new DBMS.Postgres(cn);

        var d = dbms.ResolveDestinationOrDefault(name, () =>
        {
            var c = dbms.CreateDestination(name);
            c.HasKeyMap = false;
            c.AllowOffset = false;
            return c;
        });
        return d;
    }
}
