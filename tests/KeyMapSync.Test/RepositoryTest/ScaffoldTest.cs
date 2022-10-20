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
create table if not exists public.accounts (
    account_id bigserial not null primary key,
    journal_date date not null,
    accounts_name text not null,
    price int8 not null,
    remakrs text null,
    create_at timestamp null default current_timestamp
)";
        DbExecute(cn =>
        {
            var rep = new DestinationRepository(new Postgres(), cn) { Logger = x => Output.WriteLine(x) };
            cn.Execute(ddl);

            var c = rep.Save("public", "accounts");

            Assert.Equal("public", c.SchemaName);
            Assert.Equal("accounts", c.TableName);
            Assert.Equal(6, c.Columns.ToList().Count);
            Assert.Equal("account_id", c.SequenceConfig.Column);
            Assert.Equal("nextval('accounts_account_id_seq'::regclass)", c.SequenceConfig.Command);
            Assert.True(c.AllowOffset);
            Assert.NotEqual(0, c.DestinationId);
        });
    }

    [Fact]
    public void SaveDatasource()
    {
        var ddl = @"
create table if not exists public.sales (
    sale_id bigserial not null primary key,
    sale_date date not null,
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
    s.sale_date as journal_date
    , 'sales' as accounts_name
    , s.price
    , s.sale_id
from
    sales s";

            var c = rep.SaveAsRoot("test", "accounts <- sales", "public", "sales", sql, "public", "accounts");
            rep.Save(c);
            return c;
        });

        var ext = DbExecute(cn =>
        {
            var rep = new DatasourceRepository(db, cn) { Logger = Logger };
            var sql = @"
select
    journal_date
    , 'reverse' as accounts_name
    , s.price * -1 as price
from
    temporary_table s";
            var c = rep.SaveAsExtension("accounts[reverse] <- x", sql, "public", "accounts");

            root.Extensions.Add(c);
            rep.Save(root);
            return c;
        });

        DbExecute(cn =>
        {
            var rep = new DatasourceRepository(db, cn) { Logger = Logger };
            var sql = @"
select
    journal_date
    , '10x' as accounts_name
    , s.price * 10 as price
from
    temporary_table s";
            var c = rep.SaveAsExtension("accounts[10x] <- x", sql, "public", "accounts");

            root.Extensions.Add(c);
            ext.Extensions.Add(c);
            rep.Save(root);
            rep.Save(ext);
            return c;
        });
    }
}
