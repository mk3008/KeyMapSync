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

namespace KeyMapSync.Test.RepositoryTest;

public class DatasourceRepositoryTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public DatasourceRepositoryTest(ITestOutputHelper output)
    {
        Output = output;
    }

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
            var rep = new DatasourceRepository(new Postgres(), cn) { Logger = x => Output.WriteLine(x) };
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

            //Assert.Equal("public", c.SchemaName);
            //Assert.Equal("accounts", c.TableName);
            //Assert.Equal(sql, c.Query);
            //Assert.Equal("account_id", c.KeyColumns);
            //Assert.Equal("nextval('accounts_account_id_seq'::regclass)", c.Sequence.Command);
            //Assert.True(c.AllowOffset);


            //c.Description = "sample";

            //rep.Save(c);
        });
    }
}
