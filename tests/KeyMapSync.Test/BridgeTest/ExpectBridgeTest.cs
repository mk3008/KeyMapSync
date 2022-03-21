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

public class ExpectBridgeTest
{
    private readonly ITestOutputHelper Output;

    public ExpectBridgeTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void BuildExtendWithQueryTest_ExistsVersionRange()
    {
        var ds = EcShopSaleDetail.GetDatasource();

        var root = new Abutment(ds, new BridgeCommand() { Datasource = ds });
        var bridge = new ExpectPier(root);
        bridge.AddFilter(new ExistsVersionRangeCondition(1, 2));
        var val = bridge.ToSelectQuery();

        var expect = $@"select
    __map.ec_shop_sale_detail_id
    , __p.*
from integration_sale_detail __p
inner join integration_sale_detail__map_ec_shop_sale_detail __map on __p.integration_sale_detail_id = __map.integration_sale_detail_id
where
    exists (select * from integration_sale_detail__sync ___sync where ___sync.version_id between :_min_version_id and :_max_version_id and __p.integration_sale_detail_id = ___sync.integration_sale_detail_id)";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void BuildExtendWithQueryTest_ExistsVersion()
    {
        var ds = EcShopSaleDetail.GetDatasource();

        var root = new Abutment(ds, new BridgeCommand() { Datasource = ds });
        var bridge = new ExpectPier(root);
        bridge.AddFilter(new ExistsVersionRangeCondition());
        var val = bridge.ToSelectQuery();

        var expect = $@"select
    __map.ec_shop_sale_detail_id
    , __p.*
from integration_sale_detail __p
inner join integration_sale_detail__map_ec_shop_sale_detail __map on __p.integration_sale_detail_id = __map.integration_sale_detail_id
where
    exists (select * from integration_sale_detail__sync ___sync where :_min_version_id <= ___sync.version_id and __p.integration_sale_detail_id = ___sync.integration_sale_detail_id)";

        Assert.Equal(expect, val);
    }
}
