using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KeyMapSync.Transform;
public class DifferentCondition : IFilterable
{
    public IList<string> InspectionColumns { get; } = new List<string>();

    public string RemarksColumn { get; set; } = "_remarks";

    /// <summary>
    /// ex.
    /// case when 
    ///     (ds.ec_shop_sale_detail_id is null) then 'deleted'
    /// else
    ///   case when (_e.sale_date is null and ds.sale_date is not null) 
    ///             or
    ///             (_e.sale_date is not null and ds.sale_date is null)
    ///             or 
    ///             (_e.sale_date <> ds.sale_date is null) then 'sale_date is changed, '
    ///   end
    /// end as remarks
    /// </summary>
    /// <returns></returns>   
    public string toDifferentRemarksSql(IBridge sender)
    {
        if (string.IsNullOrEmpty(RemarksColumn)) return null;
        if (!(sender is ChangedBridge)) throw new NotSupportedException();

        var changed = sender as ChangedBridge;
        var expect = changed.Owner;
        var ds = sender.Datasource;

        return BuildRemarksSql(ds.Alias, ds.KeyColumns, changed.ExpectAlias);
    }

    /// <summary>
    /// ex.
    /// (ds.ec_shop_sale_detail_id is null)
    /// or 
    /// (_e.sale_date is null and ds.sale_date is not null)
    /// or
    /// (_e.sale_date is not null and ds.sale_date is null)
    /// or 
    /// (_e.sale_date <> ds.sale_date is null)
    /// </summary>
    /// <returns></returns>    
    public Filter ToFilter(IBridge sender)
    {
        if (!(sender is ChangedBridge)) throw new NotSupportedException();

        var changed = sender as ChangedBridge;
        var ds = sender.Datasource;

        return new Filter()
        {
            Condition = BuildWhereSql(ds.Alias, ds.KeyColumns, changed.ExpectAlias)
        };
    }

    public string BuildRemarksSql(string datasourceAlias, IEnumerable<string> datasourceKeys, string expectAlias)
    {
        var sb = new StringBuilder();
        sb.Append($"case when {ConvertToDeleteCondition(datasourceAlias, datasourceKeys)} then 'deleted'");
        sb.AppendLine().Append("else ");

        var isFirst = true;
        foreach (var item in InspectionColumns)
        {
            if (!isFirst) sb.AppendLine().Append("     || ");
            sb.Append("case when ");
            sb.Append(ConvertToDiffCondition(datasourceAlias, expectAlias, item));
            sb.Append($" then '{item} is changed, ' end");
            isFirst = false;
        }

        sb.AppendLine().Append($"end as {RemarksColumn}");

        return sb.ToString();
    }

    public string BuildWhereSql(string datasourceAlias, IEnumerable<string> datasourceKeys, string expectAlias)
    {
        var sb = new StringBuilder();
        sb.Append("(");
        sb.AppendLine().Append("    ");
        sb.Append(ConvertToDeleteCondition(datasourceAlias, datasourceKeys));

        foreach (var item in InspectionColumns)
        {
            sb.AppendLine().Append("or  ");
            sb.Append(ConvertToDiffCondition(datasourceAlias, expectAlias, item));
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
        return $"coalesce(({expectAlias}.{column} = {datasourceAlias}.{column}) or ({expectAlias}.{column} is null and {datasourceAlias}.{column} is null), false)";
    }
}