using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class AdditionalPier : PierBase
{
    public AdditionalPier(IBridge bridge) : base(bridge)
    {
        Filter.Add(new NotExistsKeyMapCondition());
    }

    public override string CteName => "_added";

    private List<string> GetColumns()
    {
        var dst = this.GetDestination();
        var seq = dst.Sequence;

        var cols = new List<string>();
        if (seq != null) cols.Add($"{seq.Command} as {seq.Column}");
        cols.Add($"{this.GetInnerAlias()}.*");

        return cols;
    }

    public override string ToSelectQuery()
    {
        var view = this.GetAbutment().ViewName;

        var cmd = new SelectCommand()
        {
            Tables = { $"{view} {this.GetInnerAlias()}" },
            Columns = GetColumns(),
            WhereText = Filter.ToCondition(this).ToWhereSqlText(),
        };

        return cmd.ToSqlCommand().CommandText;
    }
}

