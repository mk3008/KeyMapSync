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

public class CreateTableTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public CreateTableTest(ITestOutputHelper output)
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
    public void CreateConfigsTable()
    {
        DbExecute(cn =>
        {
            var rep = new SystemConfigRepository(new Postgres(), cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_configs");
        });
    }

    [Fact]
    public void CreateTransactionsTable()
    {
        DbExecute(cn =>
        {
            var rep = new TransactionRepository(cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_transactions");
        });
    }


    [Fact]
    public void CreateProcessesTable()
    {
        DbExecute(cn =>
        {
            var rep = new ProcessRepository(cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_processes");
        });
    }

    [Fact]
    public void CreateDestinationsTable()
    {
        DbExecute(cn =>
        {
            var rep = new DestinationRepository(new Postgres(), cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_destinations");
        });
    }

    [Fact]
    public void CreateDatasourcesTable()
    {
        DbExecute(cn =>
        {
            var rep = new DatasourceRepository(new Postgres(), cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_datasources");
        });
    }
}
