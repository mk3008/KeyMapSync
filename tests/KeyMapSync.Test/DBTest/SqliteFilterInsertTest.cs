using Dapper;
using KeyMapSync.DBMS;
using KeyMapSync;
using KeyMapSync.Filtering;
using KeyMapSync.Test.Model;
using KeyMapSync.Test.Script;
using KeyMapSync.Transform;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using System.Diagnostics;

namespace KeyMapSync.Test.DBTest;

public class SqliteFilterInsertTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./filter_insert_test.sqlite;Cache=Shared";

    public SqliteFilterInsertTest(ITestOutputHelper output)
    {
        Output = output;

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            foreach (var item in Integration.InitializeSql.Split(";"))
            {
                cn.Execute(item);
            };
            foreach (var item in EcShop.InitializeSql.Split(";"))
            {
                cn.Execute(item);
            };
            foreach (var item in EcShop.CreateDataSql.Split(";"))
            {
                cn.Execute(item);
            };
            foreach (var item in Store.InitializeSql.Split(";"))
            {
                cn.Execute(item);
            };
            foreach (var item in Store.CreateDataSql.Split(";"))
            {
                cn.Execute(item);
            };
        }
    }

    [Fact]
    public void InsertTest()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        IDBMS db = new SQLite();
        var sync = new Synchronizer(db);
        sync.BeforeSqlExecute += OnBeforeSqlExecute;

        dynamic prm = new Dictionary<string, object>();
        prm[":ec_shop_article_id"] = 10;
        var f = new CustomFilter()
        {
            Condition = "{0}.ec_shop_article_id = :ec_shop_article_id",
            Parameter = prm
        };

        // Execute DDL test
        var cnt = 0;
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cnt = sync.Insert(cn, ds, filter: f);
        }

        Assert.Equal(3, cnt);

        // rerun
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cnt = sync.Insert(cn, ds, filter: f);
        }

        Assert.Equal(0, cnt);

        // parameter change
        prm[":ec_shop_article_id"] = 20;
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cnt = sync.Insert(cn, ds, filter: f);
        }

        Assert.Equal(3, cnt);

        sync.BeforeSqlExecute -= OnBeforeSqlExecute;
    }

    private void OnBeforeSqlExecute(object sender, SqlEventArgs e)
    {
        Debug.WriteLine(e.GetSqlInfo());
        Debug.WriteLine("--");
    }
}

