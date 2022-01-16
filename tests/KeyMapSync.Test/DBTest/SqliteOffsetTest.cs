using Dapper;
using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using KeyMapSync.Test.Model;
using KeyMapSync.Test.Script;
using KeyMapSync.Transform;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KeyMapSync.Test.DBTest;

public class SqliteOffsetTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./offset_test.sqlite;Cache=Shared";

    public SqliteOffsetTest(ITestOutputHelper output)
    {
        Output = output;

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            foreach (var item in Integration.InitializeSql.Split(";")) cn.Execute(item);
            foreach (var item in EcShop.InitializeSql.Split(";")) cn.Execute(item);
            foreach (var item in EcShop.CreateDataSql.Split(";")) cn.Execute(item);
            foreach (var item in Store.InitializeSql.Split(";")) cn.Execute(item);
            foreach (var item in Store.CreateDataSql.Split(";")) cn.Execute(item);
        }
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
            cn.Execute("update ec_shop_sale_detail set quantity = quantity + 1, price = unit_price * (quantity + 1) where ec_shop_sale_detail_id = 1");
        }
    }


    private int Sync(Datasource ds)
    {
        IDBMS db = new SQLite();
        var sync = new Synchronizer() { Dbms = db };
        sync.BeforeSqlExecute += OnBeforeSqlExecute;

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            return sync.Insert(cn, ds);
        }
    }

    private void OnBeforeSqlExecute(object sender, SqlEventArgs e)
    {
        Output.WriteLine(e.GetSqlInfo());
    }

    private int Offset(Datasource ds, IFilter validateFilter)
    {
        IDBMS db = new SQLite();
        var sync = new Synchronizer() { Dbms = db };
        sync.BeforeSqlExecute += OnBeforeSqlExecute;

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
        var storeDs = StoreSaleDetail.GetDatasource();

        //insert ecshop
        var cnt = Sync(ecShopDs);
        Assert.Equal(11, cnt);

        var validateFilter = new ExistsVersionRangeCondition();

        // no chnage
        cnt = Offset(ecShopDs, validateFilter);
        Assert.Equal(0, cnt);

        //insert store
        cnt = Sync(storeDs);
        Assert.Equal(11, cnt);

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
        cnt = Offset(ecShopDs, validateFilter);
        Assert.Equal(1, cnt);


        //delete ecshop

        //hit

    }
}

