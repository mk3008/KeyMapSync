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
    public void SqlTest()
    {
        var expect = "exists (select * from sync _sync where _sync.version_id between :_min_version_id and :_max_version_id and _origin.id = _sync.id)";

        var sync = "sync";
        var key = "id";

        Assert.Equal(expect, ExistsVersionRangeCondition.BuildSql(sync, key));
    }

    [Fact]
    public void ParameterTest()
    {
        var min = 1;
        var max = 2;
        var cnd = new ExistsVersionRangeCondition() { MinVersion = min, MaxVersion = max};

        dynamic prm = cnd.BuildParameter();        

        Assert.Equal(min, prm._min_version_id);
        Assert.Equal(max, prm._max_version_id);
    }
}

