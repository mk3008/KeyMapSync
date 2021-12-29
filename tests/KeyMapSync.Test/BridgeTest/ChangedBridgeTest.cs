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
        var work = new ExpectBridge() { Owner = root, Condition = new ExistsVersionRangeCondition() { MinVersion = 1, MaxVersion = 1 } };
        var cnd = new DifferentCondition();
        var bridge = new ChangedBridge() { Owner = work , Condition = cnd};

        var expect = @"_changed as (
    select
        _e.article_name
        , _e.unit_price
        , _e.quantity * -1 as quantity
        , _e.price * -1 as price
        , case when ds.ec_shop_sale_detail_id is null then
            'deleted'
        else
            case when coalesce((_e.sale_date = ds.sale_date) or (_e.sale_date is null and ds.sale_date is null), false) then 'sale_date is changed, ' end
            || case when coalesce((_e.ec_shop_article_id = ds.ec_shop_article_id) or (_e.ec_shop_article_id is null and ds.ec_shop_article_id is null), false) then 'ec_shop_article_id is changed, ' end
            || case when coalesce((_e.article_name = ds.article_name) or (_e.article_name is null and ds.article_name is null), false) then 'article_name is changed, ' end
            || case when coalesce((_e.unit_price = ds.unit_price) or (_e.unit_price is null and ds.unit_price is null), false) then 'unit_price is changed, ' end
            || case when coalesce((_e.quantity = ds.quantity) or (_e.quantity is null and ds.quantity is null), false) then 'quantity is changed, ' end
            || case when coalesce((_e.price = ds.price) or (_e.price is null and ds.price is null), false) then 'price is changed, ' end
        end as _remarks
    from _expect _e
    inner join integration_sale_detail__map_ec_shop_sale_detail _km on _e.integration_sale_detail_id = _km.integration_sale_detail_id
    left join ds on _km.ec_shop_sale_detail_id = ds.ec_shop_sale_detail_id
    where
        (
            ds.ec_shop_sale_detail_id is null
        or  coalesce((_e.sale_date = ds.sale_date) or (_e.sale_date is null and ds.sale_date is null), false)
        or  coalesce((_e.ec_shop_article_id = ds.ec_shop_article_id) or (_e.ec_shop_article_id is null and ds.ec_shop_article_id is null), false)
        or  coalesce((_e.article_name = ds.article_name) or (_e.article_name is null and ds.article_name is null), false)
        or  coalesce((_e.unit_price = ds.unit_price) or (_e.unit_price is null and ds.unit_price is null), false)
        or  coalesce((_e.quantity = ds.quantity) or (_e.quantity is null and ds.quantity is null), false)
        or  coalesce((_e.price = ds.price) or (_e.price is null and ds.price is null), false)
        )
)";
        var val = bridge.BuildExtendWithQuery();
        Assert.Equal(expect, val);
    }
}
