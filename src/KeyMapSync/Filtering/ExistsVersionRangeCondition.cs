using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync.Filtering;

public class ExistsVersionRangeCondition : IExistsCondition
{
    public int MinVersion { get; set; }

    public int MaxVersion { get; set; }

    /// <summary>
    /// ex.
    /// exists (
    ///     select 
    ///         *
    ///     from
    ///         integration_sale_detail__sync _sync 
    ///     where 
    ///         _sync.version_id between :_min_version and :_max_version 
    ///         and _origin.integration_sale_detail_id = _sync.integration_sale_detail_id
    ///     )
    /// </summary>
    /// <returns></returns>    
    public Filter ToFilter(IBridge sender)
    {
        var ds = sender.Datasource;
        var sync = ds.SyncName;
        var key = ds.Destination.SequenceKeyColumn;

        return new Filter()
        {
            Condition = BuildSql(sync, key),
            Parameters = BuildParameter()
        };
    }

    public static string BuildSql(string sync, string destinationKey)
    {
        return $"exists (select * from {sync} _sync where _sync.version_id between :_min_version_id and :_max_version_id and _origin.{destinationKey} = _sync.{destinationKey})";
    }

    public ExpandoObject BuildParameter()
    {
        dynamic obj = new ExpandoObject();
        obj._min_version_id = MinVersion;
        obj._max_version_id = MaxVersion;
        return obj;
    }
}
