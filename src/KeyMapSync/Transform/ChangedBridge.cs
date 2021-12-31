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

    IBridge IBridge.Owner => Owner;

    public Datasource Datasource => Owner.Datasource;

    //public string RemarksColumn { get; set; } = "_remarks";

    public string Alias => "_changed";

    public string InnerExpectAlias { get; set; } = "__e";

    public DifferentCondition Filter { get; set; } = new DifferentCondition();

    IFilter IBridge.Filter => Filter;

    public string BridgeName => Owner.BridgeName;

    public string GetWithQuery() => Owner.GetWithQuery();


    public string BuildExtendWithQuery()
    {
        var ds = Datasource;
        var dest = ds.Destination;
        var destKey = dest.SequenceKeyColumn;
        var col = GetColumns().ToString("\r\n, ").AddIndent(4);

        var sql = $@"select
{col}
from {Owner.Alias} {InnerExpectAlias}
inner join {Datasource.KeyMapName} __map on {InnerExpectAlias}.{destKey} = __map.{destKey}
left join {this.GetDatasourceAlias()} {this.GetInnerDatasourceAlias()} on {Datasource.KeyColumns.Select(x => $"__map.{x} = {this.GetInnerDatasourceAlias()}.{x}").ToString(" and ")}
{Filter.ToCondition(this).ToWhereSqlText()}";

        sql = $@"{Alias} as (
{sql.AddIndent(4)}
)";
        return sql;
    }

    private IEnumerable<string> GetColumns()
    {
        var ds = Datasource;
        var dest = ds.Destination;

        var cols = new List<string>();
        //origin key
        cols.Add($"{InnerExpectAlias}.{dest.SequenceKeyColumn}");
        //offset key
        cols.Add($"{dest.SequenceCommand} as {dest.OffsetColumnPrefix}{dest.SequenceKeyColumn}");
        //offset remarks
        if (!string.IsNullOrEmpty(dest.RemarksColumn)) cols.Add(Filter.BuildRemarksSql(this));
        //renewal key
        cols.Add($"case when {this.GetInnerDatasourceAlias()}.{ds.KeyColumns.First()} is null then null else count(*) over() + {dest.SequenceCommand} end as {dest.RenewalColumnPrefix}{dest.SequenceKeyColumn}");
        //renewal values
        cols.Add($"{this.GetInnerDatasourceAlias()}.*");

        return cols;
    }
}

