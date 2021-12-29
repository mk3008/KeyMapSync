using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

/// <summary>
/// TODO
/// </summary>
public class ChangedBridge : IBridge
{
    public ExpectBridge Owner { get; set; }

    public Datasource Datasource => Owner.Datasource;

    public string RemarksColumn { get; set; } = "_remarks";

    public string Alias => "_changed";

    public string ExpectAlias { get; set; } = "_e";

    public string BridgeName => Owner.BridgeName;

    /// <summary>
    /// ex.
    /// with 
    /// ds as (...), 
    /// _expect as (...),
    /// _changed as (
    ///    select
    ///        _e.integration_sale_detail_id, _e.sale_date, _e.ec_shop_article_id, _e.article_name, _e.unit_price, _e.quantity * -1 as quantity, _e.price * -1 as price, current_timestamp as create_timestamp, case when ds.ec_shop_sale_detail_id is null then 'row:deleted' end || case when _e.sale_date <> ds.sale_date then 'sale_date:diff,' end || case when _e.unit_price <> ds.unit_price then 'unit_price:diff,' end || case when _e.price <> ds.price then 'price:diff,' end as _validate_remarks
    ///    from
    ///        _expect _e
    ///        left join ds _e.ec_shop_sale_detail_id = ds.ec_shop_sale_detail_id
    ///    where
    ///        ds.ec_shop_sale_detail_id is null or _e.sale_date <> ds.sale_date or _e.unit_price <> ds.unit_price or _e.price <> ds.price
    /// ),
    /// </summary>
    /// <returns></returns>
    public string GetWithQuery()
    {
        var datasourceAlias = Datasource.Alias;
        var ds = Datasource;
        var dest = ds.Destination;
        var destKey = dest.SequenceKeyColumn;

        var q1 = dest.Columns.Where(x => ds.Columns.Contains(x)).Where(x => !ds.SingInversionColumns.Contains(x)).Select(x => $"{ExpectAlias}.{x}");
        var q2 = dest.Columns.Where(x => ds.Columns.Contains(x)).Where(x => ds.SingInversionColumns.Contains(x)).Select(x => $"{ExpectAlias}.{x} * -1 as {x}");
        var col = string.Empty;
        if (string.IsNullOrEmpty(RemarksColumn))
        {
            col = q1.Union(q2).ToString(", ");
        }
        else
        {
            var q3 = new string[] { toDifferentRemarksSql() };
            col = q1.Union(q2).Union(q3).ToString(", ");
        };

        var sql = $@"{Owner.GetWithQuery()},
{Alias} as (
    select {col}
    from {Owner.Alias} {ExpectAlias}
    inner join {Datasource.KeyMapName} _km on {ExpectAlias}.{destKey} = _km.{destKey}
    left join {datasourceAlias} on {Datasource.KeyColumns.Select(x => $"_km.{x} = {datasourceAlias}.{x}").ToString(" and ")}
    {ToFilter().ToWhereSqlText()}
)";
        return sql;
    }

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
    public string toDifferentRemarksSql()
    {
        if (string.IsNullOrEmpty(RemarksColumn)) return null;

        var ds = Datasource;
        var sb = new StringBuilder();
        sb.AppendLine().Append($"        case when {ConvertToDeleteCondition()} then 'deleted'");
        sb.AppendLine().Append("        else ");
        var isFirst = true;
        var cols = ds.Columns.Where(x => !ds.KeyColumns.Contains(x)).Where(x => !ds.InspectionIgnoreColumns.Contains(x));
        foreach (var item in cols)
        {
            if (!isFirst) sb.AppendLine().Append("            || ");
            sb.Append("case when ");
            sb.Append(ConvertToDiffCondition(item));
            sb.Append($" then '{item} is changed, ' end ");
            isFirst = false;
        }

        sb.AppendLine().Append($"        end as {RemarksColumn}");

        return sb.ToString();
    }

    private string ConvertToDeleteCondition()
    {
        return $"({ExpectAlias}.{Datasource.Destination.SequenceKeyColumn} is null)";
    }

    private string ConvertToDiffCondition(string column)
    {
        var datasource = Datasource.Alias;

        var sb = new StringBuilder();

        sb.Append("(");
        sb.Append($"({ExpectAlias}.{column} is null and {datasource}.{column} is not null)");
        sb.Append(" or ");
        sb.Append($"({ExpectAlias}.{column} is not null and {datasource}.{column} is null)");
        sb.Append(" or ");
        sb.Append($"({ExpectAlias}.{column} <> {datasource}.{column})");
        sb.Append(")");

        return sb.ToString();
    }

    private Filter ToFilter()
    {
        var sb = new StringBuilder();
        sb.Append(ConvertToDeleteCondition());

        var ds = Datasource;
        var cols = ds.Columns.Where(x => !ds.KeyColumns.Contains(x)).Where(x => !ds.InspectionIgnoreColumns.Contains(x));
        foreach (var item in cols)
        {
            sb.AppendLine().Append("        or ");
            sb.Append(ConvertToDiffCondition(item));
        }

        return new Filter()
        {
            Condition = $"({sb})"
        };
    }

}

