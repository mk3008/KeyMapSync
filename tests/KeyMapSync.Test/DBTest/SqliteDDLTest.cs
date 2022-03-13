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

public class SqliteDDLTest
{

    private readonly ITestOutputHelper Output;

    public SqliteDDLTest(ITestOutputHelper output)
    {
        Output = output;
    }

//    [Fact]
//    public void KeyMapDDLTest()
//    {
//        var ds = EcShopSaleDetail.GetDatasource();
//        IDBMS db = new SQLite();

//        var expect = @"create table if not exists integration_sale_detail__map_ec_shop_sale_detail
//(
//    integration_sale_detail_id integer not null
//    , ec_shop_sale_detail_id integer not null
//    , primary key(ec_shop_sale_detail_id)
//)";
//        var val = db.ToKeyMapDDL(ds);

//        Assert.Equal(expect, val);
//    }

//    [Fact]
//    public void SyncDDLTest()
//    {
//        var ds = EcShopSaleDetail.GetDatasource();
//        IDBMS db = new SQLite();

//        var expect = @"create table if not exists integration_sale_detail__sync
//(
//    integration_sale_detail_id integer not null
//    , version_id integer not null
//    , primary key(integration_sale_detail_id)
//)";
//        var val = db.ToSyncDDL(ds);

//        Assert.Equal(expect, val);
//    }

//    [Fact]
//    public void VersionDDLTest()
//    {
//        var ds = EcShopSaleDetail.GetDatasource();
//        IDBMS db = new SQLite();

//        var expect = @"create table if not exists integration_sale_detail__version
//(
//    version_id integer primary key autoincrement
//    , datasource_name text not null
//    , create_timestamp timestamp not null default current_timestamp
//)";
//        var val = db.ToVersionDDL(ds);

//        Assert.Equal(expect, val);
//    }

//    [Fact]
//    public void OffsetDDLTest()
//    {
//        var ds = EcShopSaleDetail.GetDatasource();
//        IDBMS db = new SQLite();

//        var expect = @"create table if not exists integration_sale_detail__offset
//(
//    integration_sale_detail_id integer not null
//    , offset_integration_sale_detail_id integer not null
//    , renewal_integration_sale_detail_id integer
//    , remarks text not null
//    , primary key(integration_sale_detail_id)
//    , unique(offset_integration_sale_detail_id)
//    , unique(renewal_integration_sale_detail_id)
//)";
//        var val = db.ToOffsetDDL(ds);

//        Assert.Equal(expect, val);
//    }
}

