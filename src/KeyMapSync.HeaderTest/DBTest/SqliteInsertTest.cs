﻿using Dapper;
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

public class SqliteInsertTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./insert_test.sqlite;Cache=Shared";

    public SqliteInsertTest(ITestOutputHelper output)
    {
        Output = output;

        using var cn = new SQLiteConnection(CnString);
        cn.Open();

        Integration.InitializeSql.Split(";").ToList().ForEach(item => cn.Execute(item));

        EcShop.InitializeSql.Split(";").ToList().ForEach(item => cn.Execute(item));
        EcShop.CreateDataSql.Split(";").ToList().ForEach(item => cn.Execute(item));

        Store.InitializeSql.Split(";").ToList().ForEach(item => cn.Execute(item));
        Store.CreateDataSql.Split(";").ToList().ForEach(item => cn.Execute(item));
    }

    [Fact]
    public void InsertTest()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        IDBMS db = new SQLite();
        var sync = new Synchronizer(db);

        // Execute DDL test
        var cnt = 0;
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cnt = sync.Insert(cn, ds);
        }

        Assert.Equal(11, cnt);

        // rerun
        using (var cn = new SQLiteConnection(CnString))
        {
            cn.Open();
            cnt = sync.Insert(cn, ds);
        }

        Assert.Equal(0, cnt);
    }
}

