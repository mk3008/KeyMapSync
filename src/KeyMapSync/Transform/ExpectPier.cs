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
    public ExpectPier(IBridge bridge, IFilter? validateFilter = null) : base(bridge)
    {
        if (validateFilter != null) Filter.Add(validateFilter);
    }

    public override string CteName => "_expect";

    public override string ToSelectQuery()
    {
        var ds = this.GetDatasource();
        var dest = ds.Destination;

        var tbl = dest.KeyMapConfig?.ToDbTable(ds);
        if (tbl == null) throw new InvalidOperationException();

        var keymap = tbl.Table;
        var header = HeaderInfo();
        var seq = dest.Sequence;

        //columns
        var columns = ds.KeyColumns.Select(x => $"__map.{x}").ToList();
        header.Columns.ForEach(x => columns.Add(x));
        columns.Add($"{this.GetInnerAlias()}.*");

        //from table, join talbes
        var tables = new List<string>();
        tables.Add($"{dest.TableName} {this.GetInnerAlias()}");
        header.Tables.ForEach(x => tables.Add(x));
        if (seq != null) tables.Add($"inner join {keymap} __map on {this.GetInnerAlias()}.{seq.Column} = __map.{seq.Column}");

        var column = columns.ToString("\r\n, ").AddIndent(4);
        var table = tables.ToString("\r\n");

        var sql = $@"select
{column}
from {table}
{Filter.ToCondition(this).ToWhereSqlText()}";

        return sql;
    }

    private (List<string> Columns, List<string> Tables) HeaderInfo()
    {
        var dest = this.GetDestination();

        var columns = new List<string>();
        var joins = new List<string>();

        foreach (var item in dest.Groups)
        {
            var alias = item.GetInnerAlias();
            var sql = @$"inner join {item.TableName} {alias} on {this.GetInnerAlias()}.{item.Sequence.Column} = {alias}.{ item.Sequence.Column}";
            item.GetColumnsWithoutKey().ForEach(x => columns.Add($"{alias}.{x}"));
            joins.Add(sql);
        }

        return (columns, joins);
    }
}

