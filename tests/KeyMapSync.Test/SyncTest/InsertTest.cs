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

namespace KeyMapSync.Test.SyncTest;

public class InsertTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public InsertTest(ITestOutputHelper output)
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

    private void SetTestData()
    {
        DbExecute(cn =>
        {
            var rep = new TransactionRepository(cn);
            rep.CreateTableOrDefault();
            cn.Execute("truncate table sales");

            var sql = @"insert into sales (
    sale_date,
    price,
    remakrs
)
values
('2000-01-01', 100, 'test1'),
('2000-01-02', 200, 'test2'),
('2000-01-03', 300, 'test3'),
('2000-01-04', 400, 'test4'),
('2000-01-05', 500, 'test5')";
            cn.Execute(sql);
        });

    }

    [Fact]
    public void Test()
    {
        SetTestData();

        var logger = (string s) => Output.WriteLine(s);

        DbExecute(cn =>
        {
            var db = new Postgres();

            logger.Invoke("load sysconfig");
            var sysconfig = (new SystemConfigRepository(db, cn) { Logger = logger }).Load();

            logger.Invoke("load datasource");
            var rep = (new DatasourceRepository(new Postgres(), cn) { Logger = logger });
            var d = rep.FindByName("accounts <- sales", "public", "accounts");

            if (d == null) throw new NullReferenceException();

            logger.Invoke("sync datasource");
            var sync = new Synchronizer(sysconfig, db) { Logger = logger };
            sync.CreateTable(cn, d);
            var result = sync.Insert(cn, d);
        });
    }
}