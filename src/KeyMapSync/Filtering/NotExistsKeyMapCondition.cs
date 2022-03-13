﻿using KeyMapSync.Entity;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync.Filtering;

public class NotExistsKeyMapCondition : IFilter
{
    public string ToCondition(IPier sender)
    {
        var ds = sender.GetDatasource();
        var config = ds.Destination.KeyMapConfig;
        if (config == null) throw new NotSupportedException($"keymap is not supportes.(table:{ds.Destination.DestinationTableName})");

        var keymap = config.ToDbTable(ds).Table;
        var datasourceAlias = sender.GetInnerAlias();
        var datasourceKeys = ds.KeyColumns;
        return BuildSql(keymap, datasourceAlias, datasourceKeys);
    }

    public static string BuildSql(string keymap, string datasourceAlias, IEnumerable<string> datasourceKeys)
    {
        var sql = $"not exists (select * from {keymap} ___map where {datasourceKeys.Select(key => $"{datasourceAlias}.{key} = ___map.{key}").ToString(" and ")})";
        return sql;
    }

    public Dictionary<string, object>? ToParameter()
    {
        return null;
    }
}
