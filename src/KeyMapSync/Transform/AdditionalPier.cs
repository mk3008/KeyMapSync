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

    public override string Name => "_added";

    public override string BuildCurrentSelectQuery()
    {
        var view = this.GetAbutment().Name;
        var dst = this.GetDestination();

        var cols = new List<string>();
        cols.Add($"{dst.SequenceCommand} as {dst.SequenceKeyColumn}");
        cols.Add($"{InnerAlias}.*");
        var col = cols.ToString("\r\n, ").AddIndent(4);

        var whereText = Filter.ToCondition(this).ToWhereSqlText();

        var sql = $@"select
{col}
from {view} {InnerAlias}
{whereText}";

        return sql;
    }
}

