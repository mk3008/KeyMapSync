using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

[Obsolete("use FilterContainer")]
public class FilterPier : PierBase
{
    public FilterPier(IBridge bridge, string name = null) : base(bridge)
    {
        if (name != null) AliasName = name;
    }

    public override string Name => AliasName;

    private string AliasName { get; set; } = "_filtered";

    public override string BuildCurrentSelectQuery()
    {
        var view = this.GetAbutment();

        var sql = $@"select
    __ds.*
from {view} __ds
{Filter.ToCondition(this).ToWhereSqlText()}";

        return sql;
    }
}

