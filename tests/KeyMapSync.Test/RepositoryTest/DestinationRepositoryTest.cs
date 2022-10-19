using Dapper;
using KeyMapSync.DBMS;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KeyMapSync.Test.RepositoryTest;

public class DestinationRepositoryTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public DestinationRepositoryTest(ITestOutputHelper output)
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
            var rep = new DestinationRepository(new Postgres(), cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_destinations");
        });
    }

    [Fact]
    public void TestScafold()
    {
        var ddl = @"create table if not exists public.accounts (
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

            var c = rep.FindByName("public", "accounts");
            if (c == null)
            {
                c = rep.GetScaffold("public", "accounts");

                Assert.Equal("public", c.SchemaName);
                Assert.Equal("accounts", c.TableName);
                Assert.Equal(6, c.Columns.ToList().Count);
                Assert.Equal("account_id", c.SequenceConfig.Column);
                Assert.Equal("nextval('accounts_account_id_seq'::regclass)", c.SequenceConfig.Command);
                Assert.True(c.AllowOffset);

                rep.Save(c);
            }

            c.Description = "sample";

            rep.Save(c);
        });
    }


    //[Fact]
    //public void DatasourceTest()
    //{
    //    DbExecute(cn =>
    //    {
    //        var rep = new DatasourceRepository(new Postgres(), cn);
    //        rep.CreateTableOrDefault();
    //        cn.Execute("select * from kms_datasources");
    //    });
    //}
}
