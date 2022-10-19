using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static KeyMapSync.DBMS.DbColumn;

namespace KeyMapSync.Test.Model.Postgres;

internal class DatasourceManager
{
    public static Datasource GetSales(IDbConnection cn)
    {
        var name = "sales";
        var dbms = new DBMS.Postgres();
        var sourcerep = new DatasourceRepository(dbms, cn);
        var destrep = new DestinationRepository(dbms, cn);

        var sql = @"
select
    s.sales_id
    
    , s.sales_date as journal_date
    , s.price
    , 'sales' as accounts_name
    , s.product_name as remarks
    
    , 'accounts receivable' as debit_accounts_name
    , '' as debit_remarks
from
    sales s";

        var d = sourcerep.GetScaffold("", name, sql);
        d.Destination = destrep.FindByTableName("accounts");

        var d = dbms.ResolveDatasourceOrDefault(name, () =>
        {
            var c = new Datasource();
            c.TableName = name;
            c.DestinationName = "accounts";
            c.KeyColumns["sales_id"] = Types.Numeric;
            c.Query =

            c.Extensions.Add(GetDebitAccounts(cn, ));

            return c;
        });
        return d;
    }

    private static Datasource GetDebitAccounts(IDbConnection cn)
    {
        var name = "ext_debit_accounts";
        var dbms = new DBMS.Postgres(cn);
        var d = dbms.ResolveDatasourceOrDefault(name, () =>
        {
            var c = new Datasource();
            c.TableName = name;
            c.DestinationName = "accounts";
            c.Query = @"
select
    accounts_id as credit_accounts_id    
    , b.journal_date
    , b.price   
    , b.debit_accounts_name as accounts_name
    , b.debit_remarks as remarks
from
    bridge b";

            c.Extensions.Add(GetDebitTrans(cn));

            return c;
        });
        return d;
    }

    private static Datasource GetDebitTrans(IDbConnection cn)
    {
        var name = "ext_debit_trans";
        var dbms = new DBMS.Postgres(cn);
        var d = dbms.ResolveDatasourceOrDefault(name, () =>
        {
            var c = new Datasource();
            c.TableName = name;
            c.DestinationName = "ext_account_static_trans";
            c.Query = @"
select
    b.accounts_id as debit_accounts_id
    , b.credit_accounts_id
from
    bridge b";

            return c;
        });
        return d;
    }

    public static Datasource GetPayments(IDbConnection cn)
    {
        var name = "payments";
        var dbms = new DBMS.Postgres(cn);
        var d = dbms.ResolveDatasourceOrDefault(name, () =>
        {
            var c = new Datasource();
            c.TableName = name;
            c.KeyColumns["payment_id"] = Types.Numeric;
            c.DestinationName = "accounts";
            c.Query = @"
select
    p.payment_id
    
    , p.payment_date as journal_date
    , p.price
    , 'cash' as accounts_name
    , '' as remarks
    
    , 'accounts receivable' as credit_accounts_name
    , '' as credit_remarks
from
    payments p";

            c.Extensions.Add(GetCreditAccounts(cn));

            return c;
        });
        return d;
    }

    private static Datasource GetCreditAccounts(IDbConnection cn)
    {
        var name = "ext_credit_accounts";
        var dbms = new DBMS.Postgres(cn);
        var d = dbms.ResolveDatasourceOrDefault(name, () =>
        {
            var c = new Datasource();
            c.TableName = name;
            c.DestinationName = "accounts";
            c.Query = @"
select
    accounts_id as debit_accounts_id    
    , b.journal_date
    , b.price   
    , b.credit_accounts_name as accounts_name
    , b.credit_remarks as remarks
from
    bridge b";

            c.Extensions.Add(GetCreditTrans(cn));

            return c;
        });
        return d;
    }

    private static Datasource GetCreditTrans(IDbConnection cn)
    {
        var name = "ext_credit_trans";
        var dbms = new DBMS.Postgres(cn);
        var d = dbms.ResolveDatasourceOrDefault(name, () =>
        {
            var c = new Datasource();
            c.TableName = name;
            c.DestinationName = "ext_account_static_trans";
            c.Query = @"
select
    b.accounts_id as credit_accounts_id
    , b.debit_accounts_id
from
    bridge b";

            return c;
        });
        return d;
    }
}
