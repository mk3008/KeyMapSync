using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public abstract class PierBase : IPier
{
    public PierBase(IBridge bridge)
    {
        PreviousBridge = bridge;
        if (bridge is IPier) PreviousPrier = (IPier)bridge;
    }

    protected IBridge PreviousBridge { get; }

    public IPier PreviousPrier { get; }

    public abstract string Name { get; }

    public FilterContainer Filter { get; } = new FilterContainer();

    public IAbutment GetAbutment() => PreviousBridge.GetAbutment();

    public IPier GetCurrentPier() => this;

    public string GetWithQuery()
    {
        var w = PreviousPrier?.GetWithQuery();
        w = (w == null) ? "with\r\n" : $"{w},\r\n";

        var currentSql = BuildCurrentSelectQuery();
        // with sql
        currentSql = $@"{Name} as (
{currentSql.AddIndent(4)}
)";

        var sql = $@"{w}{currentSql}";
        return sql;
    }

    public abstract string BuildCurrentSelectQuery();

    public string InnerAlias { get; } = "__p";
}