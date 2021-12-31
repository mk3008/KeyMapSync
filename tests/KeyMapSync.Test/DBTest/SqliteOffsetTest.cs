﻿using Dapper;
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
    public void OffsetTest()
    {
/*        var ds = EcShopSaleDetail.GetDatasource();
        IDBMS db = new SQLite();
        var sync = new Synchronizer() { Dbms = db };

        // Execute DDL test
        var cnt = 0;
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cnt = sync.Insert(cn, ds);
        }

        Assert.Equal(11, cnt);

        // rerun
        var validateFilter = new ExistsVersionRangeCondition() { MinVersion = 1, MaxVersion = 1 };
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cnt = sync.Offset(cn, ds, validateFilter);
        }

        Assert.Equal(0, cnt);*/
    }
}

