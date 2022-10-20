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

public class DatasourceRepositoryTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public DatasourceRepositoryTest(ITestOutputHelper output)
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

    [Fact]
    public void TestCreateTable()
    {
        DbExecute(cn =>
        {
            var rep = new DatasourceRepository(new Postgres(), cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_datasources");
        });
    }

    [Fact]
    public void TestScafold()
    {
        var ddl = @"create table if not exists public.sales (
    sale_id bigserial not null primary key,
    sale_date date not null,
    price int8 not null,
    remakrs text null,
    create_at timestamp null default current_timestamp
)";
        var sql = @"select
    s.sale_date as journal_date
    , 'sales' as accounts_name
    , s.price
    , s.sale_id
from
    sales s";

        DbExecute(cn =>
        {
            var rep = new DatasourceRepository(new Postgres(), cn) { Logger = Logger };
            cn.Execute(ddl);

            var c = rep.FindByName("accounts <- sales", "public", "accounts");
            if (c == null)
            {
                c = rep.GetScaffold("accounts <- sales", "public", "sales", sql, "public", "accounts");
                c.GroupName = "test";
                rep.Save(c);
            }

            c.Description = "test";

            rep.Save(c);
        });
    }

    [Fact]
    public void AddExtensionTest()
    {
        var sql = @"select
    journal_date
    , 'reverse' as accounts_name
    , s.price * -1 as price
from
    temporary_table s";

        DbExecute(cn =>
        {
            var db = new Postgres();
            var rep = new DatasourceRepository(db, cn) { Logger = Logger };

            var parent = rep.FindByName("accounts <- sales", "public", "accounts");
            if (parent == null) throw new Exception();

            var dsrep = new DestinationRepository(db, cn) { Logger = Logger };
            var c = new Datasource();
            c.ParentDatasourceId = parent.DatasourceId;
            c.Query = sql;
            c.Destination = dsrep.FindByName("public", "accounts");

            rep.Save(c);
        });
    }
}
