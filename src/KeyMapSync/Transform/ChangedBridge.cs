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

    public DifferentCondition Condition { get; set; }

    public string BridgeName => Owner.BridgeName;

    public string GetWithQuery() => Owner.GetWithQuery();


    public string BuildExtendWithQuery()
    {
        var datasourceAlias = Datasource.Alias;
        var ds = Datasource;
        var dest = ds.Destination;
        var destKey = dest.SequenceKeyColumn;

        var q1 = dest.Columns.Where(x => ds.Columns.Contains(x)).Where(x => !ds.SingInversionColumns.Contains(x)).Select(x => $"{ExpectAlias}.{x}");
        var q2 = dest.Columns.Where(x => ds.Columns.Contains(x)).Where(x => ds.SingInversionColumns.Contains(x)).Select(x => $"{ExpectAlias}.{x} * -1 as {x}");
        var q = q1.Union(q2);

        if (!string.IsNullOrEmpty(RemarksColumn))
        {
            var q3 = new string[] { Condition.BuildRemarksSql(this) };
            q = q.Union(q3);
        }
        var col = q.ToString("\r\n, ").Indent(4);

        var sql = $@"select
{col}
from {Owner.Alias} {ExpectAlias}
inner join {Datasource.KeyMapName} _km on {ExpectAlias}.{destKey} = _km.{destKey}
left join {datasourceAlias} on {Datasource.KeyColumns.Select(x => $"_km.{x} = {datasourceAlias}.{x}").ToString(" and ")}
{Condition.ToFilter(this).ToWhereSqlText()}";

        sql = $@"{Alias} as (
{sql.Indent(4)}
)";
        return sql;
    }
}

