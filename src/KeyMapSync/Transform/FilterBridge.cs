using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class FilterBridge : IBridge
{
    public IBridge Owner { get; set; }

    /// <summary>
    /// ex.
    /// _table_name 
    /// </summary>
    public IList<Filter> Filters { get; } = new List<Filter>();

    public string Alias => "_filtered";

    public string BridgeName => Owner.BridgeName;

    public Datasource Datasource => Owner.Datasource;

    public string GetWithQuery() => Owner.GetWithQuery();

    public string BuildExtendWithQuery()
    {
        var sql = $@"select
    {Owner.Alias}.*
from {Owner.Alias}
{Filters.ToWhereSqlText()}";

        sql = $@"{Alias} as (
{sql.AddIndent(4)}
)";
        return sql;
    }
}

