using KeyMapSync.DBMS;
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
    public ChangedPier(ExpectPier bridge) : base(bridge)
    {
        Filter.Add(DiffCondition);
    }

    public override string CteName => "_changed";

    public string InnerExpectAlias { get; } = "__e";

    private DifferentCondition DiffCondition { get; set; } = new DifferentCondition();

    public override string ToSelectQuery()
    {
        var config = this.GetDestination().KeyMapConfig;
        if (config == null) throw new NotSupportedException($"keymap config is null.(destination:{this.GetDestination().TableName})");

        var ds = this.GetDatasource();
        var dest = this.GetDestination();
        var view = Abutment.ViewName;
        var keymap = config.ToDbTable(ds).Table;

        var seq = dest.Sequence;
        var keymapJoin = $"inner join {keymap} __map on {InnerExpectAlias}.{seq.Column} = __map.{seq.Column}";
        var viewjoin = $"left join {view} {this.GetInnerAlias()} on {ds.KeyColumns.Select(x => $"__map.{x} = {this.GetInnerAlias()}.{x}").ToString(" and ")}";

        var cmd = new SelectCommand()
        {
            Tables = { $"{PreviousBridge.ViewOrCteName} {InnerExpectAlias}", keymapJoin, viewjoin },
            Columns = GetColumns(),
            WhereText = Filter.ToCondition(this).ToWhereSqlText()
        };

        return cmd.ToSqlCommand().CommandText;
    }

    private List<string> GetColumns()
    {
        var ds = this.GetDatasource();
        var dest = ds.Destination;

        var config = dest.KeyMapConfig;
        if (config == null) throw new NotSupportedException();
        var offsetconfig = config.OffsetConfig;
        if (offsetconfig == null) throw new NotSupportedException();

        var cols = new List<string>();
        //origin key
        cols.Add($"{InnerExpectAlias}.{dest.Sequence.Column}");
        //origin header key
        dest.Groups.ForEach(x => cols.Add($"{InnerExpectAlias}.{x.Sequence.Column}"));
        //offset key
        cols.Add($"{dest.Sequence.Command} as {offsetconfig.OffsetColumnPrefix}{dest.Sequence.Column}");
        //offset remarks
        cols.Add(DiffCondition.BuildRemarksSql(this));
        //renewal key
        cols.Add($"case when {this.GetInnerAlias()}.{ds.KeyColumns.First()} is null then null else count(*) over() + {dest.Sequence.Command} end as {offsetconfig.RenewalColumnPrefix}{dest.Sequence.Column}");
        //renewal values
        cols.Add($"{this.GetInnerAlias()}.*");

        return cols;
    }
}

