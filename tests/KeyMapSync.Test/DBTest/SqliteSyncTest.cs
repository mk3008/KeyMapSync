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

public class SqliteSyncTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./database.sqlite;Cache=Shared";

    public SqliteSyncTest(ITestOutputHelper output)
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
    public void Sync()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        IDBMS db = new SQLite();
        var sync = new Synchronizer() { Dbms = db };

        // Execute DDL test
        using (var cn = new SQLiteConnection(CnString))
        {
            sync.CreateSystemTable(cn, ds);
        }

        // create temporary table test
        var tmp = "tmp";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        var bridge = new Additional() { Owner = root, Filter = new NotExistsKeyMapCondition() };
        using (var cn = new SQLiteConnection(CnString))
        {
            sync.CreateTemporaryTable(cn, bridge);
        }





    }
}

