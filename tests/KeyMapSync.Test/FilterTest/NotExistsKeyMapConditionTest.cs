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

public class NotExistsKeyMapConditionTest
{
    private readonly ITestOutputHelper Output;

    public NotExistsKeyMapConditionTest(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void SqlTest()
    {
        var expect = "not exists (select * from keymap _km where ds.key1 = _km.key1)";

        var keymap = "keymap";
        var alias = "ds";
        var keys = new string[] { "key1" };
        var val = NotExistsKeyMapCondition.BuildSql(keymap, alias, keys);

        Assert.Equal(expect, val);
    }

    [Fact]
    public void SqlTest_MultipleKey()
    {
        var expect = "not exists (select * from keymap _km where ds.key1 = _km.key1 and ds.key2 = _km.key2)";

        var keymap = "keymap";
        var alias = "ds";
        var keys = new string[] { "key1", "key2" };
        var val = NotExistsKeyMapCondition.BuildSql(keymap, alias, keys);

        Assert.Equal(expect, val);
    }
}

