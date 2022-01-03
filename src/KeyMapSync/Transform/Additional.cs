using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class Additional : IBridge
{
    public Additional()
    {
        Filter = new FilterContainer();
        (Filter as FilterContainer).Add(new NotExistsKeyMapCondition());
    }

    public IBridge Owner { get; set; }

    public Datasource Datasource => Owner.Datasource;

    public IFilter Filter { get; }

    public void AddFilter(IFilter f)
    {
        if (f == null) return;
        (Filter as FilterContainer).Add(f);
    }

    public string Alias => "_added";

    public string BridgeName => Owner.BridgeName;

    public string GetWithQuery()
    {
        var w = Owner.GetWithQuery();
        w = (w == null) ? "with\r\n" : $"{w},\r\n";

        var sql = $@"{w}{BuildExtendWithQuery()}";
        return sql;
    }

    public string BuildExtendWithQuery()
    {
        var dst = Datasource.Destination;

        var cols = new List<string>();
        cols.Add($"{dst.SequenceCommand} as {dst.SequenceKeyColumn}");
        cols.Add($"{this.GetInnerDatasourceAlias()}.*");
        var col = cols.ToString("\r\n, ").AddIndent(4);
        var sql = $@"select
{col}
from {Owner.Alias} {this.GetInnerDatasourceAlias()}
{Filter.ToCondition(this).ToWhereSqlText()}";
        sql = $@"{Alias} as (
{sql.AddIndent(4)}
)";
        return sql;
    }

    public BridgeRoot GetRoot() => Owner.GetRoot();
}

