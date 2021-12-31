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

namespace KeyMapSync.Test.FilterTest;

public class DifferentConditionTest
{
    private readonly ITestOutputHelper Output;

    public DifferentConditionTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void RemarksSqlTest()
    {
        var cnd = new DifferentCondition();
        var val = cnd.BuildRemarksSql("ds", new string[] { "key1" }, "_e", new string[] { "val1" });
        var expect = @"case when ds.key1 is null then
    'deleted'
else
    case when not coalesce((_e.val1 = ds.val1) or (_e.val1 is null and ds.val1 is null), false) then 'val1 is changed, ' else '' end
end as _remarks";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void RemarksSqlTest_MultipleKey()
    {
        var cnd = new DifferentCondition();
        var val = cnd.BuildRemarksSql("ds", new string[] { "key1", "key2" }, "_e", new string[] { "val1" });
        var expect = @"case when ds.key1 is null then
    'deleted'
else
    case when not coalesce((_e.val1 = ds.val1) or (_e.val1 is null and ds.val1 is null), false) then 'val1 is changed, ' else '' end
end as _remarks";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void RemarksSqlTest_MultipleValue()
    {
        var cnd = new DifferentCondition();
        var val = cnd.BuildRemarksSql("ds", new string[] { "key1" }, "_e", new string[] { "val1", "val2" });
        var expect = @"case when ds.key1 is null then
    'deleted'
else
    case when not coalesce((_e.val1 = ds.val1) or (_e.val1 is null and ds.val1 is null), false) then 'val1 is changed, ' else '' end
    || case when not coalesce((_e.val2 = ds.val2) or (_e.val2 is null and ds.val2 is null), false) then 'val2 is changed, ' else '' end
end as _remarks";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void WhereSqlTest()
    {
        var cnd = new DifferentCondition();
        var val = cnd.BuildWhereSql("ds", new string[] { "key1" }, "_e", new string[] { "val1" });
        var expect = @"(
    ds.key1 is null
or  not coalesce((_e.val1 = ds.val1) or (_e.val1 is null and ds.val1 is null), false)
)";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void WhereSqlTest_MultipleKey()
    {
        var cnd = new DifferentCondition();
        var val = cnd.BuildWhereSql("ds", new string[] { "key1", "key2" }, "_e", new string[] { "val1" });
        var expect = @"(
    ds.key1 is null
or  not coalesce((_e.val1 = ds.val1) or (_e.val1 is null and ds.val1 is null), false)
)";

        Assert.Equal(expect, val);
    }

    [Fact]
    public void WhereSqlTest_MultipleValue()
    {
        var cnd = new DifferentCondition();
        var val = cnd.BuildWhereSql("ds", new string[] { "key1" }, "_e", new string[] { "val1", "val2" });
        var expect = @"(
    ds.key1 is null
or  not coalesce((_e.val1 = ds.val1) or (_e.val1 is null and ds.val1 is null), false)
or  not coalesce((_e.val2 = ds.val2) or (_e.val2 is null and ds.val2 is null), false)
)";

        Assert.Equal(expect, val);
    }

}

