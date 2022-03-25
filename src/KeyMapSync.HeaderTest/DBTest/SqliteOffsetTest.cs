using Dapper;
using KeyMapSync;
using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using KeyMapSync.HeaderTest.Model;
using KeyMapSync.HeaderTest.Script;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KeyMapSync.HeaderTest.DBTest;

public class SqliteOffsetTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./offset_test.sqlite;Cache=Shared";

    public SqliteOffsetTest(ITestOutputHelper output)
    {
        Output = output;

        using var cn = new SQLiteConnection(CnString);
        cn.Open();

        Integration.InitializeSql.Split(";").ToList().ForEach(item => cn.Execute(item));

        EcShop.InitializeSql.Split(";").ToList().ForEach(item => cn.Execute(item));
        EcShop.CreateDataSql.Split(";").ToList().ForEach(item => cn.Execute(item));
    }

    internal void AddEcshopExtendSale()
    {
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            foreach (var item in EcShop.CreateExtendDataSql.Split(";")) cn.Execute(item);
        }
    }

    internal void UpdateQuantity()
    {
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            var cnt = cn.Execute("update ec_shop_sale_detail set quantity = quantity + 1, price = unit_price * (quantity + 1) where ec_shop_sale_detail_id = 1");
        }
    }

    internal void DeleteRow()
    {
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cn.Execute("delete from ec_shop_sale_detail where ec_shop_sale_detail_id = 2");
        }
    }


    private int Sync(Datasource ds)
    {
        IDBMS db = new SQLite();
        var sync = new Synchronizer(db);
        sync.BeforeSqlExecute += OnBeforeSqlExecute;

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            return sync.Insert(cn, ds);
        }
    }

    private void OnBeforeSqlExecute(object? sender, SqlEventArgs? e)
    {
        if (e != null) Output.WriteLine(e.GetSqlInfo());
    }

    private int Offset(Datasource ds, IFilter validateFilter, bool isLogging = false)
    {
        IDBMS db = new SQLite();
        var sync = new Synchronizer(db);
        if (isLogging) sync.BeforeSqlExecute += OnBeforeSqlExecute;

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            return sync.Offset(cn, ds, validateFilter);
        }
    }


    [Fact]
    public void OffsetTest()
    {
        var ecShopDs = EcShopSaleDetail.GetDatasource();

        //insert ecshop
        var cnt = Sync(ecShopDs);
        Assert.Equal(11, cnt);

        var validateFilter = new ExistsVersionRangeCondition();

        // no chnage
        cnt = Offset(ecShopDs, validateFilter);
        Assert.Equal(0, cnt);

        //ecshop is not changed
        cnt = Offset(ecShopDs, validateFilter);
        Assert.Equal(0, cnt);

        //insert ecshop
        AddEcshopExtendSale();

        //ecshop is not changed (Inserts are not subject to verification.)
        cnt = Offset(ecShopDs, validateFilter);
        Assert.Equal(0, cnt);

        //update ecsho
        UpdateQuantity();

        //hit
        Output.WriteLine("--- offset ---");
        cnt = Offset(ecShopDs, validateFilter);
        Assert.Equal(1, cnt);

        //delete ecshop
        DeleteRow();

        //hit
        cnt = Offset(ecShopDs, validateFilter, true);
        Assert.Equal(1, cnt);

    }
}

