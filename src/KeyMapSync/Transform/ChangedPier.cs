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
public class ChangedPier : PierBase
{
    public ChangedPier(ExpectPier bridge) : base(bridge) {
        Filter.Add(DiffCondition);
    }

    public override string Name => "_changed";

    public string InnerExpectAlias { get; } = "__e";

    private DifferentCondition DiffCondition { get; set; } = new DifferentCondition();

    public override string BuildCurrentSelectQuery()
    {
        var ds = this.GetDatasource();
        var dest = this.GetDestination();
        var view = GetAbutment().Name;
        var keymap = ds.KeyMapName;

        var destKey = dest.SequenceKeyColumn;
        var col = GetColumns().ToString("\r\n, ").AddIndent(4);

        var sql = $@"select
{col}
from {PreviousBridge.Name} {InnerExpectAlias}
inner join {keymap} __map on {InnerExpectAlias}.{destKey} = __map.{destKey}
left join {view} {InnerAlias} on {ds.KeyColumns.Select(x => $"__map.{x} = {InnerAlias}.{x}").ToString(" and ")}
{Filter.ToCondition(this).ToWhereSqlText()}";

        return sql;
    }

    private IList<string> GetColumns()
    {
        var ds = this.GetDatasource();
        var dest = ds.Destination;

        var cols = new List<string>();
        //origin key
        cols.Add($"{InnerExpectAlias}.{dest.SequenceKeyColumn}");
        //offset key
        cols.Add($"{dest.SequenceCommand} as {dest.OffsetColumnPrefix}{dest.SequenceKeyColumn}");
        //offset remarks
        if (!string.IsNullOrEmpty(dest.OffsetRemarksColumn)) cols.Add(DiffCondition.BuildRemarksSql(this));
        //renewal key
        cols.Add($"case when {InnerAlias}.{ds.KeyColumns.First()} is null then null else count(*) over() + {dest.SequenceCommand} end as {dest.RenewalColumnPrefix}{dest.SequenceKeyColumn}");
        //renewal values
        cols.Add($"{InnerAlias}.*");

        return cols;
    }

/*    public override string ToDestinationSql()
    {
        var ds = this.GetDatasource();
        var dest = this.GetDestination();

        var toTable = dest.DestinationName;
        var fromTable = this.GetBridgeName();
        var cols = dest.Columns.Where(x => dest.SequenceKeyColumn != x).Where(x => ds.Columns.Contains(x)).ToList();

        var sql = CreateInsertSql(toTable, cols, fromTable);
        return sql;
    }*/

    public override string ToRemoveKeyMapSql()
    {
        var ds = this.GetDatasource();
        var dest = this.GetDestination();

        var toTable = ds.KeyMapName;
        var fromTable = this.GetBridgeName();
        var key = dest.SequenceKeyColumn;

        var sql = $@"delete from {toTable}
where
    exists (select * from {fromTable} t where {toTable}.{key} = t.{key});";
        return sql;
    }
}

