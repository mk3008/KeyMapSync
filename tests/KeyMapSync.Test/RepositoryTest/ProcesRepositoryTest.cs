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

public class ProcesRepositoryTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public ProcesRepositoryTest(ITestOutputHelper output)
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
            var rep = new ProcessRepository(cn);
            rep.CreateTableOrDefault();
            cn.Execute("select * from kms_processes");
        });
    }
}
