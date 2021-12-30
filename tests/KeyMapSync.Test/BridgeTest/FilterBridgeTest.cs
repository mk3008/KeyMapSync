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
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KeyMapSync.Test.BridgeTest;

public class FilterBridgeTest
{
    private readonly ITestOutputHelper Output;

    public FilterBridgeTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void BuildExtendWithQueryTest()
    {
        var f = new Filter()
        {
            Condition = "ec_shop_article_id = :_ec_shop_article_id",
        };
        dynamic prm = new ExpandoObject();
        prm._ec_shop_article_id = 1;

        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        var bridge = new FilterBridge() { Owner = root};
        bridge.Filters.Add(f);

        var val = bridge.BuildExtendWithQuery();
        var expect = $@"_filtered as (
    select
        ds.*
    from ds
    where
        ec_shop_article_id = :_ec_shop_article_id
)";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void AdditionalNestTest()
    {
        var f = new Filter()
        {
            Condition = "ec_shop_article_id = :_ec_shop_article_id",
        };
        dynamic prm = new ExpandoObject();
        prm._ec_shop_article_id = 1;

        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        var fb = new FilterBridge() { Owner = root };
        fb.Filters.Add(f);
        var bridge = new Additional() { Owner = fb, AdditionalCondition = new NotExistsKeyMapCondition() };

        var val = bridge.BuildExtendWithQuery();
        var expect = $@"_added as (
    select
        (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sales_detail' union all select 0)) as integration_sale_detail_id
        , __ds.*
    from _filtered __ds
    where
        not exists (select * from integration_sale_detail__map_ec_shop_sale_detail ___map where __ds.ec_shop_sale_detail_id = ___map.ec_shop_sale_detail_id)
)";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void ChangedBridgeNestTest()
    {
        var f = new Filter()
        {
            Condition = "ec_shop_article_id = :_ec_shop_article_id",
        };
        dynamic prm = new ExpandoObject();
        prm._ec_shop_article_id = 1;

        var ds = EcShopSaleDetail.GetDatasource();

        var root = new BridgeRoot() { Datasource = ds, BridgeName = "tmp_default_parse" };
        var fb = new FilterBridge() { Owner = root };
        fb.Filters.Add(f);
        var work = new ExpectBridge() { Owner = fb, Condition = new ExistsVersionRangeCondition() { MinVersion = 1, MaxVersion = 1 } };
        var cnd = new DifferentCondition();
        var bridge = new ChangedBridge() { Owner = work, Condition = cnd };

        var expect = @"_changed as (
    select
        __e.article_name
        , __e.unit_price
        , __e.quantity * -1 as quantity
        , __e.price * -1 as price
        , case when __ds.ec_shop_sale_detail_id is null then
            'deleted'
        else
            case when coalesce((__e.sale_date = __ds.sale_date) or (__e.sale_date is null and __ds.sale_date is null), false) then 'sale_date is changed, ' end
            || case when coalesce((__e.ec_shop_article_id = __ds.ec_shop_article_id) or (__e.ec_shop_article_id is null and __ds.ec_shop_article_id is null), false) then 'ec_shop_article_id is changed, ' end
            || case when coalesce((__e.unit_price = __ds.unit_price) or (__e.unit_price is null and __ds.unit_price is null), false) then 'unit_price is changed, ' end
            || case when coalesce((__e.quantity = __ds.quantity) or (__e.quantity is null and __ds.quantity is null), false) then 'quantity is changed, ' end
            || case when coalesce((__e.price = __ds.price) or (__e.price is null and __ds.price is null), false) then 'price is changed, ' end
        end as _remarks
    from _expect __e
    inner join integration_sale_detail__map_ec_shop_sale_detail __map on __e.integration_sale_detail_id = __map.integration_sale_detail_id
    left join _filtered __ds on _km.ec_shop_sale_detail_id = _ds.ec_shop_sale_detail_id
    where
        (
            __ds.ec_shop_sale_detail_id is null
        or  coalesce((__e.sale_date = __ds.sale_date) or (__e.sale_date is null and __ds.sale_date is null), false)
        or  coalesce((__e.ec_shop_article_id = __ds.ec_shop_article_id) or (__e.ec_shop_article_id is null and __ds.ec_shop_article_id is null), false)
        or  coalesce((__e.unit_price = __ds.unit_price) or (__e.unit_price is null and __ds.unit_price is null), false)
        or  coalesce((__e.quantity = __ds.quantity) or (__e.quantity is null and __ds.quantity is null), false)
        or  coalesce((__e.price = __ds.price) or (__e.price is null and __ds.price is null), false)
        )
)";
        var val = bridge.BuildExtendWithQuery();
        Assert.Equal(expect, val);
    }
}

