using Dapper;
using KeyMapSync;
using KeyMapSync.Test.Script;
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
using KeyMapSync.DBMS;
using SqModel;
using KeyMapSync.Test.Model.Sqlite;

namespace KeyMapSync.Test.DBTest;

public class SqliteInsertTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./filter_insert_test.sqlite;Cache=Shared";

    public SqliteInsertTest(ITestOutputHelper output)
    {
        Output = output;

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            foreach (var item in SqliteScript.InitializeSql.Split(";"))
            {
                cn.Execute(item);
            };
            foreach (var item in EcShop.CreateDataSql.Split(";"))
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

        //custom filter
        var injector = (SelectQuery sq) =>
        {
            var t = sq.FromClause;
            sq.Where.Add().Column(t, "ec_shop_article_id").Equal(":id").Parameter(":id", 10);
        };

        var sync = new Synchronizer(db);
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            sync.CreateTable(cn, ds);
        }

        sync.Logger = s => Output.WriteLine(s);

        // insert
        Results res = null;
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            res = sync.Insert(cn, ds, injector);
        }

        Assert.Equal("integration_sale_detail", (res.Collection[0] as Result).Table);
        Assert.Equal(3, (res.Collection[0] as Result).Count);

        Assert.Equal("integration_sale_detail__map_ec_shop_sale_detail", (res.Collection[1] as Result).Table);
        Assert.Equal(3, (res.Collection[1] as Result).Count);

        Assert.Equal("integration_sale_detail__sync", (res.Collection[2] as Result).Table);
        Assert.Equal(3, (res.Collection[2] as Result).Count);

        Assert.Equal("integration_sale_detail__version", (res.Collection[3] as Result).Table);
        Assert.Equal(1, (res.Collection[3] as Result).Count);

        var nres = res.Collection[4] as Results;

        Assert.Equal("integration_sale_detail_ext_ec_shop_article", (nres.Collection[0] as Result).Table);
        Assert.Equal(3, (nres.Collection[0] as Result).Count);

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            res = sync.Insert(cn, ds, injector);
        }

        Assert.Empty(res.Collection);
    }
}