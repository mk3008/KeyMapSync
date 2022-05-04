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

public class ExistsVersionRangeConditionTest
{
    private readonly ITestOutputHelper Output;

    public ExistsVersionRangeConditionTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void SqlTest_VersionRange()
    {
        var expect = "exists (select * from sync ___sync where ___sync.version_id between :_min_version_id and :_max_version_id and __ds.id = ___sync.id)";

        var sync = "sync";
        var key = "id";
        var min = 1;
        var max = 2;
        var cnd = new ExistsVersionRangeCondition(min, max);

        var val = cnd.BuildSql(sync, "__ds", key);

        Assert.Equal(expect, val);
    }

    [Fact]
    public void ParameterTest_VersionRange()
    {
        var min = 1;
        var max = 2;
        var cnd = new ExistsVersionRangeCondition(min, max);

        var prm = cnd.Parameters;

        Assert.Equal(min, prm[":_min_version_id"]);
        Assert.Equal(max, prm[":_max_version_id"]);
    }

    [Fact]
    public void SqlTest_Version()
    {
        var expect = "exists (select * from sync ___sync where :_min_version_id <= ___sync.version_id and __ds.id = ___sync.id)";

        var sync = "sync";
        var key = "id";
        var min = 1;
        var cnd = new ExistsVersionRangeCondition(min);

        var val = cnd.BuildSql(sync, "__ds", key);

        Assert.Equal(expect, val);
    }

    [Fact]
    public void ParameterTest_Version()
    {
        var min = 1;
        var cnd = new ExistsVersionRangeCondition(min);

        var prm = cnd.Parameters;

        Assert.Equal(min, prm[":_min_version_id"]);
        Assert.False(prm.ContainsKey(":_max_version_id"));
    }
}

