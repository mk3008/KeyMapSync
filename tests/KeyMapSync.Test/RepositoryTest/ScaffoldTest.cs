using KeyMapSync.DBMS;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Xunit;
using Dapper;
using KeyMapSync.Entity;

namespace KeyMapSync.Test.RepositoryTest;

public class ScaffoldTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public ScaffoldTest(ITestOutputHelper output)
    {
        Output = output;
    }

    private void Logger(string s) => Output.WriteLine(s);

    private void DbExecute(Action<IDbConnection> act)
    {
        using var cn = new NpgsqlConnection(CnString);
        cn.Open();
        using var tran = cn.BeginTransaction();
        act(cn);
        tran.Commit();
    }
    private T DbExecute<T>(Func<IDbConnection, T> fn)
    {
        using var cn = new NpgsqlConnection(CnString);
        cn.Open();
        using var tran = cn.BeginTransaction();
        var val = fn(cn);
        tran.Commit();
        return val;
    }

    [Fact]
    public void SaveDestination()
    {
        var ddl = @"
create table if not exists public.sale_slips (
    sale_slip_id bigserial not null primary key,
    base_sale_slip_id int8,
    sale_date date not null,
    shop_id int8 not null,
    client_id int8 not null,
    price int8 not null,
    create_at timestamp null default current_timestamp
)
;
create table if not exists public.sale_slip_details (
    sale_slip_detail_id bigserial not null primary key,
    sale_slip_id int8 not null,
    product_name text not null,
    price int8 not null,
    create_at timestamp null default current_timestamp
)
";
        var root = DbExecute(cn =>
        {
            ddl.Split(";").ToList().ForEach(x => cn.Execute(x));

            var rep = new DestinationRepository(new Postgres(), cn) { Logger = x => Output.WriteLine(x) };
            return rep.Save("public", "sale_slip_details");
        });

        DbExecute(cn =>
        {
            root.AllowOffset = true;
            root.InspectionIgnoreColumns = new[] { "sale_date", "product_name" };
            root.SignInversionColumns = new[] { "price" };

            var rep = new DestinationRepository(new Postgres(), cn) { Logger = x => Output.WriteLine(x) };
            rep.Save(root);
        });

        var h1 = DbExecute(cn =>
        {
            var sql = @"
select
    sale_date
    , shop_id
    , client_id
    , sum(price) as price
from
    _bridge t
group by
    sale_date
    , shop_id
    , client_id";
            var rep = new DestinationRepository(new Postgres(), cn) { Logger = x => Output.WriteLine(x) };
            var c = rep.SaveAsHeader("public", "sale_slips", new[] { "sale_date", "shop_id", "client_id" }, sql, "public", "sale_slip_details");
            return c;
        });
    }

    [Fact]
    public void SaveDatasource()
    {
        var ddl = @"
create table if not exists public.sales (
    sale_id bigserial not null primary key,
    sale_date date not null,
    shop_id int8 not null,
    client_id int8 not null,
    product_name text not null,
    price int8 not null,
    remakrs text null,
    create_at timestamp null default current_timestamp
)";
        var db = new Postgres();
        var root = DbExecute(cn =>
        {
            var rep = new DatasourceRepository(db, cn) { Logger = Logger };
            cn.Execute(ddl);

            var sql = @"
select
    s.sale_date
    , s.shop_id
    , s.client_id
    , s.product_name
    , s.price
    , s.sale_id
from
    sales s";

            var c = rep.SaveAsRoot("test", "sale_slip_details <- sales", "public", "sales", sql, "public", "sale_slip_details");
            rep.Save(c);
            return c;
        });

        var ext = DbExecute(cn =>
        {
            var rep = new DatasourceRepository(db, cn) { Logger = Logger };
            var sql = @"
select
    t.sale_date
    , t.shop_id
    , t.client_id
    , t.product_name
    , t.price * -1 as price
    , t.sale_slip_id as base_sale_slip_id
from
    _bridge t";
            var c = rep.SaveAsExtension("sale_slip_details[reverse] <- x", sql, "public", "sale_slip_details");

            root.Extensions.Add(c);
            rep.Save(root);
            return c;
        });

        DbExecute(cn =>
        {
            var rep = new DatasourceRepository(db, cn) { Logger = Logger };
            var sql = @"
select
    t.sale_date
    , t.shop_id
    , t.client_id
    , t.product_name
    , t.price * 10 as price
    , t.sale_slip_id as base_sale_slip_id
from
    _bridge t";
            var c = rep.SaveAsExtension("sale_slip_details[10x] <- x", sql, "public", "sale_slip_details");

            root.Extensions.Add(c);
            ext.Extensions.Add(c);
            rep.Save(root);
            rep.Save(ext);
            return c;
        });
    }
}
