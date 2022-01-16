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
public class ExpectPier : PierBase
{
    public ExpectPier(IBridge bridge, IFilter validateFilter = null) : base(bridge)
    {
        if (validateFilter != null) Filter.Add(validateFilter);
    }

    public override string Name => "_expect";

    public override string BuildCurrentSelectQuery()
    {
        var ds = this.GetDatasource();
        var datasourceKeys = ds.KeyColumns;
        var dest = ds.Destination;
        var keymap = ds.KeyMapName;

        var cols = datasourceKeys.Select(x => $"__map.{x}").ToList();
        cols.Add($"{this.GetInnerAlias()}.*");
        var col = cols.ToString("\r\n, ").AddIndent(4);

        var sql = $@"select
{col}
from {dest.DestinationName} {this.GetInnerAlias()}
inner join {keymap} __map on {this.GetInnerAlias()}.{dest.SequenceKeyColumn} = __map.{dest.SequenceKeyColumn}
{Filter.ToCondition(this).ToWhereSqlText()}";

        return sql;
    }
}

