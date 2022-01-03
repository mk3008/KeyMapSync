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

public class SqliteSyntaxTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./syntax_test.sqlite;Cache=Shared";

    public SqliteSyntaxTest(ITestOutputHelper output)
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
    public void SqlSyntaxTest()
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
        var tmp = "tmp_additional";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        var bridge = new Additional() { Owner = root };

        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            using (var tran = cn.BeginTransaction())
            {
                sync.CreateTemporaryTable(cn, bridge, false);
                sync.InsertDestination(cn, bridge);
                sync.InsertKeyMap(cn, bridge);
                sync.InsertSync(cn, bridge);
                sync.InsertVersion(cn, bridge);
                sync.InsertExtension(cn, bridge);
            }
        }
    }

    //TODO Filter Test

    //TODO Offset Test

    //TODO Filter and Offset Test
}

