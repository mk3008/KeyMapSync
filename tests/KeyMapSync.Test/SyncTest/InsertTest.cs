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
using SqModel;
using Utf8Json;
using Utf8Json.Resolvers;

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

            var sql = @"insert into sales (
    sale_date
    , shop_id
    , client_id
    , product_name
    , price
    , remakrs
)
values
      ('2000-01-01', 1, 100, 'apple'  , 100, '')
    , ('2000-01-01', 1, 100, 'orange' , 300, '')
    , ('2000-01-01', 1, 200, 'tea'    , 500, '')
    , ('2000-01-01', 1, 200, 'apple'  , 600, '')
    , ('2000-01-01', 2, 100, 'coffee' , 200, '')";
            cn.Execute(sql);
        });

    }

    private void SetOffsetData()
    {
        DbExecute(cn =>
        {
            var sql = @"
update sales set price = 120 where product_name = 'orange'
;
update sales set product_name = 'apple2' where product_name = 'apple'
;
delete from sales where product_name = 'coffee'
";
            cn.Execute(sql);
        });

    }

    [Fact]
    public void TestInsertWithTestData()
    {
        SetTestData();
        TestInsert();
    }

    [Fact]
    public void TestInsert()
    {
        var logger = (string s) => Output.WriteLine(s);

        var injector = (SelectQuery q, Datasource _) =>
        {
            var t = q.FromClause;
            q.Where.Add().Column(t, "price").Comparison("<>", 0);
        };

        DbExecute(cn =>
        {
            var db = new Postgres();

            logger.Invoke("load sysconfig");
            var sysconfig = (new SystemConfigRepository(db, cn)).Load();//{ Logger = logger }

            logger.Invoke("load datasource");
            var rep = (new DatasourceRepository(new Postgres(), cn)); // { Logger = logger }
            var d = rep.FindByName("sale_slip_details <- sales", "public", "sale_slip_details");

            if (d == null) throw new Exception($"Datasource is not found.");

            //debug
            //d.Extensions.Clear();

            logger.Invoke("sync datasource");
            var sync = new Synchronizer(sysconfig, db) { Logger = logger };

            logger.Invoke("*create table");
            sync.AllowLogging = false;
            sync.CreateTable(cn, d);

            logger.Invoke("*insert");
            sync.AllowLogging = true;
            var result = sync.Insert(cn, d, injector: injector);

            //result
            var text = JsonSerializer.ToJsonString(result, StandardResolver.ExcludeNull);
            logger($"result : {text}");
        });
    }

    [Fact]
    public void TestRenewWithUpdateDat()
    {
        SetOffsetData();
        TestRenew();
    }

    [Fact]
    public void TestRenew()
    {
        var logger = (string s) => Output.WriteLine(s);

        var injector = (SelectQuery q, Datasource _) =>
        {
            var t = q.FromClause;
            q.Where.Add().Column(t, "price").Comparison("<>", 0);
        };

        DbExecute(cn =>
        {
            var db = new Postgres();

            logger.Invoke("load sysconfig");
            var sysconfig = (new SystemConfigRepository(db, cn)).Load();//{ Logger = logger }

            logger.Invoke("load datasource");
            var rep = (new DatasourceRepository(new Postgres(), cn)); // { Logger = logger }
            var d = rep.FindByName("sale_slip_details <- sales", "public", "sale_slip_details");

            if (d == null) throw new Exception($"Datasource is not found.");

            logger.Invoke("sync datasource");
            var sync = new Synchronizer(sysconfig, db) { Logger = logger };

            logger.Invoke("*create table");
            sync.AllowLogging = false;
            sync.CreateTable(cn, d);

            logger.Invoke("*renew");
            sync.AllowLogging = true;
            var result = sync.Renew(cn, d, injector: injector);

            //result
            var text = JsonSerializer.ToJsonString(result, StandardResolver.ExcludeNull);
            logger($"result : {text}");
        });
    }

    [Fact]
    public void TestOffset()
    {
        var logger = (string s) => Output.WriteLine(s);

        var injector = (SelectQuery q, Datasource _) =>
        {
            var t = q.FromClause;
            q.Where.Add().Column(t, "price").Comparison("<>", 0);
        };

        DbExecute(cn =>
        {
            var db = new Postgres();

            logger.Invoke("load sysconfig");
            var sysconfig = (new SystemConfigRepository(db, cn)).Load();//{ Logger = logger }

            logger.Invoke("load datasource");
            var rep = (new DatasourceRepository(new Postgres(), cn)); // { Logger = logger }
            var d = rep.FindByName("sale_slip_details <- sales", "public", "sale_slip_details");

            if (d == null) throw new Exception($"Datasource is not found.");

            logger.Invoke("sync datasource");
            var sync = new Synchronizer(sysconfig, db) { Logger = logger };

            logger.Invoke("*create table");
            sync.AllowLogging = false;
            sync.CreateTable(cn, d);

            logger.Invoke("*offset");
            sync.AllowLogging = true;
            var result = sync.Offset(cn, d, injector: injector);

            //result
            var text = JsonSerializer.ToJsonString(result, StandardResolver.ExcludeNull);
            logger($"result : {text}");
        });
    }
}