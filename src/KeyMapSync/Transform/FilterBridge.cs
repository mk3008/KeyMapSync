using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

[Obsolete("use FilterContainer")]
public class FilterBridge : IBridge
{
    public IBridge Owner { get; set; }

    /// <summary>
    /// ex.
    /// _table_name 
    /// </summary>
    public FilterContainer FilterContainer { get; } = new FilterContainer();

    public IFilter Filter => FilterContainer;

    public string Alias => "_filtered";

    public string BridgeName => Owner.BridgeName;

    public Datasource Datasource => Owner.Datasource;

    public string GetWithQuery() => Owner.GetWithQuery();

    public string BuildExtendWithQuery()
    {
        var sql = $@"select
    {Owner.Alias}.*
from {Owner.Alias}
{Filter.ToCondition(this).ToWhereSqlText()}";

        sql = $@"{Alias} as (
{sql.AddIndent(4)}
)";
        return sql;
    }
}

