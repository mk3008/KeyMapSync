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

public class BridgeRootTest
{
    private readonly ITestOutputHelper Output;

    public BridgeRootTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void ToSqlTest()
    {
        /*
        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new Abutment(ds, tmp);

        var expect = @"create temporary table tmp_parse
as
select
    __v.version_id
    , __ds.*
from _kms_v_ec_shop_sale_detail __ds
cross join (select (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail__version' union all select 0)) + 1 as version_id) __v;";
        var val = root.ToTemporaryDdl();
        Assert.Equal(expect, val);*/
    }
}
