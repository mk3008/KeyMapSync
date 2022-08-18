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
    public ExistsVersionRangeCondition(int minVersion = 1, int? maxVersion = null)
    {
        MinVersion = minVersion;
        MaxVersion = maxVersion;

        Parameters = new Dictionary<string, object>();
        Parameters.Add(":_min_version_id", MinVersion);
        if (MaxVersion.HasValue) Parameters.Add(":_max_version_id", MaxVersion.Value);
    }

    public int MinVersion { get; }

    public int? MaxVersion { get; }

    public Dictionary<string, object>? Parameters { get; }

    public string? ConditionInfo => BuildSql("SYNC_TABLE", "DATASOURCE", "DESTINATION_ID");

    public string ToCondition(IPier sender)
    {
        var datasourceAlias = sender.AliasName;

        var ds = sender.GetDatasource();
        var vconfig = ds.Destination.VersioningConfig;
        if (vconfig == null) throw new InvalidOperationException();

        var tbl = vconfig.SyncConfig.ToDbTable(ds, vconfig);

        return BuildSql(tbl.Table, datasourceAlias, ds.Destination.Sequence.Column);
    }

    public string BuildSql(string sync, string datasourceAlias, string destinationKey)
    {
        var cnd = "";
        if (MaxVersion.HasValue)
        {
            cnd = "___sync.version_id between :_min_version_id and :_max_version_id";
        }
        else
        {
            cnd = ":_min_version_id <= ___sync.version_id";
        }
        var sql = $"exists (select * from {sync} ___sync where {cnd} and {datasourceAlias}.{destinationKey} = ___sync.{destinationKey})";

        return sql;
    }
}
