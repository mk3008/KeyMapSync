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

        var root = new BridgeRoot() { Datasource = ds, BridgeName = "tmp_default_parse" };
        var bridge = new ExpectBridge() { Owner = root, Condition = new ExistsVersionRangeCondition() { MinVersion = 1, MaxVersion = 1 } };

        var val = bridge.BuildExtendWithQuery();
        var expect = $@"_expect as (
    select
        _km.ec_shop_sale_detail_id
        , _origin.*
    from integration_sale_detail _origin
    inner join integration_sale_detail__map_ec_shop_sale_detail _km on _origin.integration_sale_detail_id = _km.integration_sale_detail_id
    where
        exists (select * from integration_sale_detail__sync _sync where _sync.version_id between :_min_version_id and :_max_version_id and _origin.integration_sale_detail_id = _sync.integration_sale_detail_id)
)";

        Assert.Equal(expect, val);
    }
}
