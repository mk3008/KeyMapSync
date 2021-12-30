using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync.Filtering;

public class ExistsVersionRangeCondition : IFilter
{
    public int MinVersion { get; set; }

    public int MaxVersion { get; set; }

    public string ToCondition(IBridge sender)
    {
        var ds = sender.Datasource;
        var sync = ds.Destination.SyncName;
        var key = ds.Destination.SequenceKeyColumn;

        return BuildSql(sync, key);
    }

    public static string BuildSql(string sync, string destinationKey)
    {
        return $"exists (select * from {sync} _sync where _sync.version_id between :_min_version_id and :_max_version_id and _origin.{destinationKey} = _sync.{destinationKey})";
    }

    public ExpandoObject ToParameter()
    {
        dynamic prm = new ExpandoObject();
        prm._min_version_id = MinVersion;
        prm._max_version_id = MaxVersion;
        return prm;
    }
}
