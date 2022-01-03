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
        var f = new CustomFilter()
        {
            Condition = "ec_shop_article_id = :_ec_shop_article_id",
        };
        dynamic prm = new ExpandoObject();
        prm._ec_shop_article_id = 1;

        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        var bridge = new FilterBridge() { Owner = root };
        bridge.FilterContainer.Add(f);

        var val = bridge.BuildExtendWithQuery();
        var expect = $@"_filtered as (
    select
        _kms_v_ec_shop_sale_detail.*
    from _kms_v_ec_shop_sale_detail
    where
        ec_shop_article_id = :_ec_shop_article_id
)";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void AdditionalNestTest()
    {
        dynamic prm = new ExpandoObject();
        prm._ec_shop_article_id = 1;
        var f = new CustomFilter()
        {
            Condition = "__ds.ec_shop_article_id = :_ec_shop_article_id",
            Parameter = prm
        };

        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        //var fb = new FilterBridge() { Owner = root };
        //fb.FilterContainer.Add(f);
        var bridge = new Additional() { Owner = root };
        bridge.AddFilter(f);
        var val = bridge.BuildExtendWithQuery();
        var expect = $@"_added as (
    select
        (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over() as integration_sale_detail_id
        , __ds.*
    from _kms_v_ec_shop_sale_detail __ds
    where
        not exists (select * from integration_sale_detail__map_ec_shop_sale_detail ___map where __ds.ec_shop_sale_detail_id = ___map.ec_shop_sale_detail_id)
        and __ds.ec_shop_article_id = :_ec_shop_article_id
)";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void ExpectBridgeNestTest()
    {
        dynamic prm = new ExpandoObject();
        prm._ec_shop_article_id = 1;
        var f = new CustomFilter()
        {
            Condition = "__ds.ec_shop_article_id = :_ec_shop_article_id",
            Parameter = prm
        };

        var ds = EcShopSaleDetail.GetDatasource();

        var root = new BridgeRoot() { Datasource = ds, BridgeName = "tmp_default_parse" };
        //var fb = new FilterBridge() { Owner = root };
        //fb.FilterContainer.Add(f);
        var fc = new FilterContainer();
        fc.Add(new ExistsVersionRangeCondition() { MinVersion = 1, MaxVersion = 1 });
        fc.Add(f);
        var work = new ExpectBridge() { Owner = root};
        work.AddFilter(fc);
        //var cnd = new DifferentCondition();
        //var bridge = new ChangedBridge() { Owner = work, Filter = cnd };

        var expect = @"_expect as (
    select
        __map.ec_shop_sale_detail_id
        , __ds.*
    from integration_sale_detail __ds
    inner join integration_sale_detail__map_ec_shop_sale_detail __map on __ds.integration_sale_detail_id = __map.integration_sale_detail_id
    where
        exists (select * from integration_sale_detail__sync ___sync where ___sync.version_id between :_min_version_id and :_max_version_id and __ds.integration_sale_detail_id = ___sync.integration_sale_detail_id)
        and __ds.ec_shop_article_id = :_ec_shop_article_id
)";
        var val = work.BuildExtendWithQuery();
        Assert.Equal(expect, val);
    }
}

