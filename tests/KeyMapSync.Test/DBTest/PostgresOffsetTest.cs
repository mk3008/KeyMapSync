using Dapper;
using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Test.Model.Postgres;
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

public class PostgresOffsetTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public PostgresOffsetTest(ITestOutputHelper output)
    {
        Output = output;

        using (var cn = new NpgsqlConnection(CnString))
        {
            //cn.Open();
            //foreach (var item in PostgresScript.InitializeSql.Split(";").Where(x => x != ""))
            //{
            //    cn.Execute(item);
            //};
            //foreach (var item in EcShop.CreateDataSql.Split(";").Where(x => x != ""))
            //{
            //    cn.Execute(item);
            //};
            //foreach (var item in Store.CreateDataSql.Split(";").Where(x => x != ""))
            //{
            //    cn.Execute(item);
            //};
        }
    }

    internal void UpdateQuantity()
    {
        using (var cn = new NpgsqlConnection(CnString))
        {
            cn.Open();
            cn.Execute("update ec_shop_sale_detail set quantity = quantity + 1, price = unit_price * (quantity + 1) where ec_shop_sale_detail_id = 1");
        }
    }

    internal void DeleteRow()
    {
        using (var cn = new NpgsqlConnection(CnString))
        {
            cn.Open();
            cn.Execute("delete from ec_shop_sale_detail where ec_shop_sale_detail_id = 2");
        }
    }

    [Fact]
    public void OffsetTest()
    {
        var dic = new Dictionary<string, Destination>();
        using (var cn = new NpgsqlConnection(CnString))
        {
            cn.Open();
            var lst = new List<Destination>();
            lst.Add(DestinationManager.GetAccounts(cn));
            lst.Add(DestinationManager.GetExtDebitAccounts(cn));

            lst.ForEach(x => dic[x.DestinationName] = x);
        }
        var resolver = (string x) => dic[x];

        IDBMS db = new Postgres();
        var sync = new Synchronizer(db, resolver);

        using (var cn = new NpgsqlConnection(CnString))
        {
            cn.Open();
            var ds = DatasourceManager.GetSales(cn);
            sync.CreateTable(cn, ds);
        }

        // insert
        Results res = null;

        using (var cn = new NpgsqlConnection(CnString))
        {
            cn.Open();
            using var tran = cn.BeginTransaction();
            var ds = DatasourceManager.GetSales(cn);
            res = sync.Insert(cn, ds);
            tran.Commit();
        }

        Assert.Equal(5, res.Collection.Count());

        // update change
        UpdateQuantity();

        sync.Logger = s => Output.WriteLine(s);

        using (var cn = new NpgsqlConnection(CnString))
        {
            cn.Open();
            using var tran = cn.BeginTransaction();
            var ds = DatasourceManager.GetSales(cn);
            res = sync.Offset(cn, ds);
            tran.Commit();
        }
    }
}

