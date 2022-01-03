using Dapper;
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

namespace KeyMapSync.Test.BridgeTest;

public class ChangedBridgeTest
{
    private readonly ITestOutputHelper Output;

    public ChangedBridgeTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void BuildExtendWithQueryTest_ExistsVersionRange()
    {
        var ds = EcShopSaleDetail.GetDatasource();

        var root = new BridgeRoot() { Datasource = ds, BridgeName = "tmp_default_parse" };
        var work = new ExpectBridge() { Owner = root};
        work.AddFilter( new ExistsVersionRangeCondition() { MinVersion = 1, MaxVersion = 1 });
        var cnd = new DifferentCondition();
        var bridge = new ChangedBridge() { Owner = work , Filter = cnd};

        var expect = @"_changed as (
    select
        __e.integration_sale_detail_id
        , (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over() as offset_integration_sale_detail_id
        , case when __ds.ec_shop_sale_detail_id is null then
            'deleted'
        else
            case when not coalesce((__e.sale_date = __ds.sale_date) or (__e.sale_date is null and __ds.sale_date is null), false) then 'sale_date is changed, ' else '' end
            || case when not coalesce((__e.unit_price = __ds.unit_price) or (__e.unit_price is null and __ds.unit_price is null), false) then 'unit_price is changed, ' else '' end
            || case when not coalesce((__e.quantity = __ds.quantity) or (__e.quantity is null and __ds.quantity is null), false) then 'quantity is changed, ' else '' end
            || case when not coalesce((__e.price = __ds.price) or (__e.price is null and __ds.price is null), false) then 'price is changed, ' else '' end
        end as _remarks
        , case when __ds.ec_shop_sale_detail_id is null then null else count(*) over() + (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over() end as renewal_integration_sale_detail_id
        , __ds.*
    from _expect __e
    inner join integration_sale_detail__map_ec_shop_sale_detail __map on __e.integration_sale_detail_id = __map.integration_sale_detail_id
    left join _kms_v_ec_shop_sale_detail __ds on __map.ec_shop_sale_detail_id = __ds.ec_shop_sale_detail_id
    where
        (
            __ds.ec_shop_sale_detail_id is null
        or  not coalesce((__e.sale_date = __ds.sale_date) or (__e.sale_date is null and __ds.sale_date is null), false)
        or  not coalesce((__e.unit_price = __ds.unit_price) or (__e.unit_price is null and __ds.unit_price is null), false)
        or  not coalesce((__e.quantity = __ds.quantity) or (__e.quantity is null and __ds.quantity is null), false)
        or  not coalesce((__e.price = __ds.price) or (__e.price is null and __ds.price is null), false)
        )
)";
        var val = bridge.BuildExtendWithQuery();
        Assert.Equal(expect, val);
    }
}
