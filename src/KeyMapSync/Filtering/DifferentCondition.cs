using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace KeyMapSync.Transform;
public class DifferentCondition : IFilter
{
    public string RemarksColumn { get; set; } = "_remarks";

    public string BuildRemarksSql(IPier sender)
    {
        if (string.IsNullOrEmpty(RemarksColumn)) throw new NullReferenceException(nameof(RemarksColumn));
        if (!(sender is ChangedPier changed)) throw new NotSupportedException();

        var ds = sender.GetDatasource();

        return BuildRemarksSql(changed.GetInnerAlias(), ds.KeyColumns, changed.InnerExpectAlias, ds.InspectionColumns);
    }

    public string BuildRemarksSql(string datasourceAlias, IEnumerable<string> datasourceKeys, string expectAlias, IEnumerable<string> inspectionColumns)
    {
        var sb = new StringBuilder();
        sb.Append($"case when {ConvertToDeleteCondition(datasourceAlias, datasourceKeys)} then");
        sb.AppendLine().Append("    'deleted'");
        sb.AppendLine().Append("else");
        sb.AppendLine().Append("    ");
        var isFirst = true;
        foreach (var item in inspectionColumns)
        {
            if (!isFirst) sb.AppendLine().Append("    || ");
            sb.Append("case when ");
            sb.Append(ConvertToDiffCondition(datasourceAlias, expectAlias, item));
            sb.Append($" then '{item} is changed, ' else '' end");
            isFirst = false;
        }

        sb.AppendLine().Append($"end as {RemarksColumn}");

        return sb.ToString();
    }

    public string BuildWhereSql(string ownerAlias, IEnumerable<string> datasourceKeys, string expectAlias, IEnumerable<string> inspectionColumns)
    {
        var sb = new StringBuilder();
        sb.Append("(");
        sb.AppendLine().Append("    ");
        sb.Append(ConvertToDeleteCondition(ownerAlias, datasourceKeys));

        foreach (var item in inspectionColumns)
        {
            sb.AppendLine().Append("or  ");
            sb.Append(ConvertToDiffCondition(ownerAlias, expectAlias, item));
        }
        sb.AppendLine().Append(")");

        return sb.ToString();
    }

    private string ConvertToDeleteCondition(string datasourceAlias, IEnumerable<string> keys)
    {
        return $"{datasourceAlias}.{keys.First()} is null";
    }

    private string ConvertToDiffCondition(string datasourceAlias, string expectAlias, string column)
    {
        return $"not coalesce(({expectAlias}.{column} = {datasourceAlias}.{column}) or ({expectAlias}.{column} is null and {datasourceAlias}.{column} is null), false)";
    }

    public string ToCondition(IPier sender)
    {
        if (!(sender is ChangedPier changed)) throw new NotSupportedException();

        var ds = sender.GetDatasource();

        return BuildWhereSql(sender.GetInnerAlias(), ds.KeyColumns, changed.InnerExpectAlias, ds.InspectionColumns);
    }

    public Dictionary<string, object>? ToParameter() => null;
}