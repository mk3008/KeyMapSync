using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Data;

public class VersionValidateFilter : IValidateFilter
{
    public IDatasource Datasource { get; set; }

    public long LowerLimitVersion { get; set; }

    public long UpperLimitVersion { get; set; }

    public IEnumerable<string> KeyColumns { get; set; }

    public IEnumerable<string> IgnoreColumns { get; set; } = Enumerable.Empty<string>();

    public ParameterSet ToExpectParameterSet(string SyncTableAliasName, string versionColumnName)
    {
        var ps = new ParameterSet();

        //ex. "Sync.Version between :lower_limit and :upper_limit"
        var keys = Datasource.DatasourceKeyColumns;
        ps.ConditionSqlText = $"{SyncTableAliasName}.{versionColumnName} between :lower_limit and :upper_limit)";
        ps.Parameters.Add("lower_limit", LowerLimitVersion);
        ps.Parameters.Add("upper_limit", UpperLimitVersion);

        return ps;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="dest">ex. "destination"</param>
    /// <param name="expectBridge">ex. "_bridge"</param>
    /// <param name="bridgeAliasName">ex. "_bridge"</param>
    /// <param name="offsetSourceColumnName">ex. "origin_destination_id"</param>
    /// <returns></returns>
    public ParameterSet ToRemoveParameterSet(Table dest, Table expectBridge, string bridgeAliasName, string offsetSourceColumnName)
    {
        var destaliasname = "_dest";

        var key = dest.SequenceColumn.ColumnName;
        var cols = dest.Columns.Where(x => key != x).Where(x => expectBridge.Columns.Contains(x)).Where(x => !IgnoreColumns.Contains(x));
        var lst = new List<string>();

        var keys = Datasource.DatasourceKeyColumns;

        foreach (var col in cols)
        {
            //ex. "(not (Destination.name is null and ExpectBridge.name is null) or (Destination.name = ExpectBridge.name))"
            lst.Add($"(not ({destaliasname}.{col} is null and {bridgeAliasName}.{col} is null) or ({destaliasname}.{col} = {bridgeAliasName}.{col}))");
        }

        //ex. "exists (not (Destination.name is null and ExpectBridge.name is null) or (Destination.name = ExpectBridge.name))"
        var updateDirty = $"exists (select * from {dest.TableFullName} {destaliasname} where ({lst.ToString(" or ")})";

        // ex. "not exists (select * from Destination where Destination.destination_id = ExpectBridge.offset_source_destination_id)"
        var removeDirty = $"not exists (select * from {dest.TableFullName} {destaliasname} where {destaliasname}.{key} = {bridgeAliasName}.{offsetSourceColumnName})";

        var ps = new ParameterSet();
        ps.ConditionSqlText = $"({removeDirty} or {updateDirty})";
        return ps;
    }

    public string ToRemoveCommentColumnSqlText(Table dest, Table expectBridge, string bridgeAliasName, string offsetSourceColumnName, string removeCommentColumnName)
    {
        var destaliasname = "_dest";

        var key = dest.SequenceColumn.ColumnName;
        var cols = dest.Columns.Where(x => key != x).Where(x => expectBridge.Columns.Contains(x)).Where(x => !IgnoreColumns.Contains(x));
        var lst = new List<string>();

        var keys = Datasource.DatasourceKeyColumns;

        // ex. " when not exists (select * from Destination where Destination.destination_id = ExpectBridge.offset_source_destination_id) then 'removed.'"
        lst.Add($"when not exists (select * from {dest.TableFullName} {destaliasname} where {destaliasname}.{key} = {bridgeAliasName}.{offsetSourceColumnName}) then 'removed.'");

        foreach (var col in cols)
        {
            //ex. "when (not (Destination.name is null and ExpectBridge.name is null) or (Destination.name = ExpectBridge.name)) then 'name is updated'"
            lst.Add($"when (not ({destaliasname}.{col} is null and {bridgeAliasName}.{col} is null) or ({destaliasname}.{col} = {bridgeAliasName}.{col})) then '{col} is updated.'");
        }

        // ex. "case when ... then ... when ... then ... end as remove_comment"
        var sql = $"case {lst.ToString(" ")} end as {removeCommentColumnName}";
        return sql;
    }
}

