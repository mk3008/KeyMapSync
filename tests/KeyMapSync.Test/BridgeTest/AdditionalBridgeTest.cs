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

public class AdditionalBridgeTest
{
    private readonly ITestOutputHelper Output;

    public AdditionalBridgeTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void BuildExtendWithQueryTest()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        var bridge = new Additional() { Owner = root };
        bridge.Filter = new NotExistsKeyMapCondition();

        var val = bridge.BuildExtendWithQuery();
        var expect = $@"_added as (
    select
        (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sales_detail' union all select 0)) + row_number() over() as integration_sale_detail_id
        , __ds.*
    from ds __ds
    where
        not exists (select * from integration_sale_detail__map_ec_shop_sale_detail ___map where __ds.ec_shop_sale_detail_id = ___map.ec_shop_sale_detail_id)
)";

        Assert.Equal(expect, val);
    }
}
