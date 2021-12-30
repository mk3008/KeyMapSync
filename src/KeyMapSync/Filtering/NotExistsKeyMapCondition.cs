using KeyMapSync.Entity;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync.Filtering;

public class NotExistsKeyMapCondition : IFilter
{
    public string ToCondition(IBridge sender)
    {
        var ds = sender.Datasource;
        var keymap = ds.KeyMapName;
        var datasourceAlias = sender.GetInnerDatasourceAlias();
        var datasourceKeys = ds.KeyColumns;
        return BuildSql(keymap, datasourceAlias, datasourceKeys);
    }

    public static string BuildSql(string keymap, string datasourceAlias, IEnumerable<string> datasourceKeys)
    {
        return $"not exists (select * from {keymap} ___map where {datasourceKeys.Select(key => $"{datasourceAlias}.{key} = ___map.{key}").ToString(" and ")})";
    }

    public ExpandoObject ToParameter()
    {
        return null;
    }
}
