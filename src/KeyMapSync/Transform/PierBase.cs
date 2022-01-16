using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public abstract class PierBase : BridgeBase, IPier
{
    public PierBase(IBridge bridge)
    {
        PreviousBridge = bridge;
        if (bridge is IPier) PreviousPrier = (IPier)bridge;
    }

    protected IBridge PreviousBridge { get; }

    public IPier PreviousPrier { get; }

    public FilterContainer Filter { get; } = new FilterContainer();

    public override IAbutment GetAbutment() => PreviousBridge.GetAbutment();

    public override IPier GetCurrentPier() => this;

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

    public (string commandText, IDictionary<string, object> parameter) ToCreateTableCommand(bool isTemporary = true)
    {
        var cmd = (ToCreateTableCommandText(isTemporary), ToCreateTableParameter());
        return cmd;
    }

    private string ToCreateTableCommandText(bool isTemporary = true)
    {
        var bridgeName = this.GetBridgeName();

        var command = isTemporary ? "create temporary table" : "create table";
        command = $"{command} {bridgeName}\r\nas";

        var withQuery = GetWithQuery();
        withQuery = (withQuery == null) ? null : $"\r\n{withQuery}"; ;

        var versionKey = this.GetDestination().VersionKeyColumn;
        var versionQuery = ToSelectVersionSql();

        var sql = $@"{command}{withQuery}
select
    __v.{versionKey}
    , {InnerAlias}.*
from {Name} {InnerAlias}
cross join ({versionQuery}) __v;";

        return sql;
    }

    public IDictionary<string, object> ToCreateTableParameter()
    {
        var pier = GetCurrentPier();
        if (pier == null) return null;

        var current = pier.Filter?.ToParameter();
        var previous = pier.PreviousPrier?.ToCreateTableParameter();
        return current == null ? previous : current.Merge(previous);
    }

    private string ToSelectVersionSql()
    {
        var dest = this.GetDestination();
        var sql = $"select {dest.VersionSequenceCommand} as {dest.VersionKeyColumn}";
        return sql;
    }
}