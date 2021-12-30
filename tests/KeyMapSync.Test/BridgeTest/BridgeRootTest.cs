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

public class ManagementTableTest
{
    private readonly ITestOutputHelper Output;

    public ManagementTableTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void ToSqlTest()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };

        var val = root.ToSql();
        var expect = @"with 
ds as (
    select
          sd.ec_shop_sale_detail_id
        , s.sale_date
        , sd.ec_shop_article_id
        , a.article_name
        , sd.unit_price
        , sd.quantity
        , sd.price
    from
        ec_shop_sale_detail sd
        inner join ec_shop_sale s on sd.ec_shop_sale_id = s.ec_shop_sale_id
        inner join ec_shop_article a on sd.ec_shop_article_id = a.ec_shop_article_id
)
create temporary table tmp_parse
as
select
    *
from ds;";

        Assert.Equal(expect, root.ToSql());
    }
}
