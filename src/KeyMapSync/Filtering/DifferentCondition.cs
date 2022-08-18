using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace KeyMapSync.Filtering;
public class DifferentCondition : IFilter
{
    public string RemarksColumn { get; set; } = "offset_remarks";

    public Dictionary<string, object>? Parameters => null;

    public string? ConditionInfo => BuildWhereSql("OWNER", new() { "DATASOURCE_ID" }, "EXPECT", new());

    private string GetDeleteCondition(string transed, string datasourcekey) => $"{transed}.{datasourcekey} is null";

    private string GetDiffCondition(string transed, string current, string column) => $"coalesce(({transed}.{column} = {current}.{column}) or ({transed}.{column} is null and {current}.{column} is null), false)";

    public string ToCondition(IPier sender)
    {
        if (!(sender is ChangedPier changed)) throw new InvalidProgramException();
        var ds = sender.GetDatasource();

        return BuildWhereSql(changed.TransformedAlias, ds.KeyColumns, changed.CurrentAlias, ds.InspectionColumns.ToList());
    }

    public SelectColumn ToRemarksColumn(IPier sender)
    {
        if (!(sender is ChangedPier changed)) throw new InvalidProgramException();
        var ds = sender.GetDatasource();

        return ToRemarksColumn(changed.TransformedAlias, ds.KeyColumns, changed.CurrentAlias, ds.InspectionColumns.ToList());
    }

    public SelectColumn ToRemarksColumn(string transed, List<string> datasourcekeys, string current, List<string> inspectionColumns)
    {
        var sb = new StringBuilder();
        sb.Append($"case when {GetDeleteCondition(transed, datasourcekeys.First())} then 'deleted' else ");

        var isFirst = true;
        foreach (var item in inspectionColumns)
        {
            if (!isFirst) sb.Append(" || ");
            sb.Append($"case when {GetDiffCondition(transed, current, item)} then '{item} is changed.' else '' end");
            isFirst = false;
        }
        sb.Append("end");

        var col = new SelectColumn()
        {
            ColumnName = RemarksColumn,
            ColumnCommand = sb.ToString(),
        };

        return col;
    }

    private string BuildWhereSql(string transed, List<string> datasourcekeys, string current, List<string> inspectionColumns)
    {
        var sb = new StringBuilder();
        sb.Append("(");
        sb.Append(GetDeleteCondition(transed, datasourcekeys.First()));

        foreach (var item in inspectionColumns)
        {
            sb.Append(" or ");
            sb.Append(GetDiffCondition(transed, current, item));
        }
        sb.Append(")");

        return sb.ToString();
    }
}