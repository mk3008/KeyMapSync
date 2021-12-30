using KeyMapSync.Entity;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Linq;

namespace KeyMapSync.Filtering;

public class NotExistsKeyMapCondition : IAdditionalCondition
{
    public Filter ToFilter(IBridge sender)
    {
        var ds = sender.Datasource;
        return new Filter()
        {
            Condition = BuildSql(ds.KeyMapName, sender.GetInnerDatasourceAlias(), ds.KeyColumns)
        };
    }

    public static string BuildSql(string keymap, string datasourceAlias, IEnumerable<string> datasourceKeys)
    {
        return $"not exists (select * from {keymap} ___map where {datasourceKeys.Select(key => $"{datasourceAlias}.{key} = ___map.{key}").ToString(" and ")})";
    }
}
