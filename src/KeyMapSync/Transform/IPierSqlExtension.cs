using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public static class IPierSqlExtension
{
    public static string GetInnerAlias(this IPier source)
    {
        return "__p";
    }

    public static string GetWithQuery(this IPier source)
    {
        var w = source.PreviousPrier?.GetWithQuery();
        w = (w == null) ? "with\r\n" : $"{w},\r\n";

        var currentSql = source.BuildCurrentSelectQuery();
        // with sql
        currentSql = $@"{source.Name} as (
{currentSql.AddIndent(4)}
)";

        var sql = $@"{w}{currentSql}";
        return sql;
    }

    public static (string commandText, IDictionary<string, object>? parameter) ToCreateTableCommand(this IPier source, bool isTemporary = true)
    {
        var cmd = (source.ToCreateTableCommandText(isTemporary), source.ToCreateTableParameter());
        return cmd;
    }

    private static string ToCreateTableCommandText(this IPier source, bool isTemporary = true)
    {
        var bridgeName = source.GetBridgeName();

        var command = isTemporary ? "create temporary table" : "create table";
        command = $"{command} {bridgeName}\r\nas";

        var withQuery = source.GetWithQuery();
        withQuery = (withQuery == null) ? null : $"\r\n{withQuery}"; ;

        var versionKey = source.GetDestination().VersionKeyColumn;
        var versionQuery = source.ToSelectVersionSql();
        var header = source.ToHeaderInfo();

        var columns = new List<string>();
        columns.Add($"__v.{versionKey}");
        header.Columns.ForEach(x => columns.Add(x));
        columns.Add($"{source.GetInnerAlias()}.*");

        var tables = new List<string>();
        tables.Add($"{source.Name} {source.GetInnerAlias()}");
        header.Tables.ForEach(x => tables.Add(x));
        tables.Add($"cross join ({versionQuery}) __v");

        var column = columns.ToString("\r\n, ").AddIndent(4);
        var table = tables.ToString("\r\n");

        var sql = $@"{command}{withQuery}
select
{column}
from {table};";

        return sql;
    }

    public static IDictionary<string, object>? ToCreateTableParameter(this IPier source)
    {
        var pier = source.GetCurrentPier();
        if (pier == null) return null;

        var current = pier.Filter?.ToParameter();
        var previous = pier.PreviousPrier?.ToCreateTableParameter();
        return current == null ? previous : current.Merge(previous);
    }

    private static string ToSelectVersionSql(this IPier source)
    {
        var dest = source.GetDestination();
        var sql = $"select {dest.VersionSequenceCommand} as {dest.VersionKeyColumn}";
        return sql;
    }

    private static (List<string> Columns, List<string> Tables) ToHeaderInfo(this IPier source)
    {
        var dest = source.GetDestination();

        var columns = new List<string>();
        var tables = new List<string>();

        foreach (var item in dest.Groups)
        {
            var alias = item.GetInnerAlias;
            var sql = @$"inner join (
    select
        h.*
        , {item.SequenceCommand} as {item.SequenceKeyColumn}
    from
        (
            select distinct {item.GroupColumns.ToString(", ")} from {source.Name}
        ) h
    ) {alias} on {item.GroupColumns.Select(x => $"{source.GetInnerAlias()}.{x} = {alias}.{x}").ToString(" and ")}";
            columns.Add($"{alias}.{item.SequenceKeyColumn}");
            tables.Add(sql);
        }

        return (columns, tables);
    }
}