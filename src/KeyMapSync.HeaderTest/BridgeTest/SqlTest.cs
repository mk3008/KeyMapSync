using Dapper;
using KeyMapSync;
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

namespace KeyMapSync.HeaderTest.BridgeTest;

public class SqlTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./database_sqltest.sqlite;Cache=Shared";

    public SqlTest(ITestOutputHelper output)
    {
        Output = output;

        using var cn = new SQLiteConnection(CnString);
        cn.Open();

        Integration.InitializeSql.Split(";").ToList().ForEach(item => cn.Execute(item));

        EcShop.InitializeSql.Split(";").ToList().ForEach(item => cn.Execute(item));
        EcShop.CreateDataSql.Split(";").ToList().ForEach(item => cn.Execute(item));
    }


    [Fact]
    public void NotExistsAdditional()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new Abutment(ds, new BridgeCommand() { Datasource = ds });
        var bridge = new AdditionalPier(root);

        var expect = @"create temporary table _ec_shop_sale_detail
as
with
_added as (
    select
        (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over() as integration_sale_detail_id
        , __p.*
    from _v__ec_shop_sale_detail __p
    where
        not exists (select * from integration_sale_detail__map_ec_shop_sale_detail ___map where __p.ec_shop_sale_detail_id = ___map.ec_shop_sale_detail_id)
)
select
    __p.*
    , g_integration_sale.integration_sale_id
    , __version.version_id
from _added __p
left join (
    select
        h.*
        , (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale' union all select 0)) + row_number() over() as integration_sale_id
    from
        (
            select distinct shop_id, sale_date from _added
        ) h
    ) g_integration_sale on __p.shop_id = g_integration_sale.shop_id and __p.sale_date = g_integration_sale.sale_date
cross join (select (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail__version' union all select 0)) + 1 as version_id) __version;";
        var val = bridge.ToCreateTableCommand();
        Assert.Equal(expect, val.CommandText);
    }

    [Fact]
    public void VersionOffsetParse()
    {
        var ds = EcShopSaleDetail.GetDatasource();

        var root = new Abutment(ds, new BridgeCommand() { Datasource = ds });
        var pier = new ExpectPier(root);
        pier.AddFilter(new ExistsVersionRangeCondition(1, 2));
        var bridge = new ChangedPier(pier);

        var expect = @"create temporary table _ec_shop_sale_detail
as
with
_expect as (
    select
        __map.ec_shop_sale_detail_id
        , g_integration_sale.shop_id
        , g_integration_sale.sale_date
        , __p.*
    from integration_sale_detail __p
    inner join integration_sale g_integration_sale on __p.integration_sale_id = g_integration_sale.integration_sale_id
    inner join integration_sale_detail__map_ec_shop_sale_detail __map on __p.integration_sale_detail_id = __map.integration_sale_detail_id
    where
        exists (select * from integration_sale_detail__sync ___sync where ___sync.version_id between :_min_version_id and :_max_version_id and __p.integration_sale_detail_id = ___sync.integration_sale_detail_id)
),
_changed as (
    select
        __e.integration_sale_detail_id
        , __e.integration_sale_id
        , (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over() as offset_integration_sale_detail_id
        , case when __p.ec_shop_sale_detail_id is null then
            'deleted'
        else
            case when not coalesce((__e.shop_id = __p.shop_id) or (__e.shop_id is null and __p.shop_id is null), false) then 'shop_id is changed, ' else '' end
            || case when not coalesce((__e.sale_date = __p.sale_date) or (__e.sale_date is null and __p.sale_date is null), false) then 'sale_date is changed, ' else '' end
            || case when not coalesce((__e.unit_price = __p.unit_price) or (__e.unit_price is null and __p.unit_price is null), false) then 'unit_price is changed, ' else '' end
            || case when not coalesce((__e.quantity = __p.quantity) or (__e.quantity is null and __p.quantity is null), false) then 'quantity is changed, ' else '' end
            || case when not coalesce((__e.price = __p.price) or (__e.price is null and __p.price is null), false) then 'price is changed, ' else '' end
        end as offset_remarks
        , case when __p.ec_shop_sale_detail_id is null then null else count(*) over() + (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over() end as renewal_integration_sale_detail_id
        , __p.*
    from _expect __e
    inner join integration_sale_detail__map_ec_shop_sale_detail __map on __e.integration_sale_detail_id = __map.integration_sale_detail_id
    left join _v__ec_shop_sale_detail __p on __map.ec_shop_sale_detail_id = __p.ec_shop_sale_detail_id
    where
        (
            __p.ec_shop_sale_detail_id is null
        or  not coalesce((__e.shop_id = __p.shop_id) or (__e.shop_id is null and __p.shop_id is null), false)
        or  not coalesce((__e.sale_date = __p.sale_date) or (__e.sale_date is null and __p.sale_date is null), false)
        or  not coalesce((__e.unit_price = __p.unit_price) or (__e.unit_price is null and __p.unit_price is null), false)
        or  not coalesce((__e.quantity = __p.quantity) or (__e.quantity is null and __p.quantity is null), false)
        or  not coalesce((__e.price = __p.price) or (__e.price is null and __p.price is null), false)
        )
)
select
    __p.*
    , g_integration_sale.integration_sale_id
    , __version.version_id
from _changed __p
left join (
    select
        h.*
        , (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale' union all select 0)) + row_number() over() as integration_sale_id
    from
        (
            select distinct shop_id, sale_date from _changed
        ) h
    ) g_integration_sale on __p.shop_id = g_integration_sale.shop_id and __p.sale_date = g_integration_sale.sale_date
cross join (select (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail__version' union all select 0)) + 1 as version_id) __version;";
        var val = bridge.ToCreateTableCommand();
        Assert.Equal(expect, val.CommandText);
    }
}
