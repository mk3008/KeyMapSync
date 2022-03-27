using KeyMapSync.DBMS;
using KeyMapSync.DBMS;
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

    public static string GetCteQuery(this IPier source)
    {
        var w = source.PreviousPrier?.GetCteQuery();
        w = (w == null) ? "with\r\n" : $"{w},\r\n";

        var currentSql = source.ToSelectQuery();
        // with sql
        currentSql = $@"{source.CteName} as (
{currentSql.AddIndent(4)}
)";

        var sql = $@"{w}{currentSql}";
        return sql;
    }

    public static SqlCommand ToCreateTableCommand(this IPier source)
    {
        var columns = new List<string>();
        var tables = new List<string>();
        columns.Add($"{source.GetInnerAlias()}.*");
        tables.Add($"{source.CteName} {source.GetInnerAlias()}");

        //header
        var header = source.ToHeaderJoinInfo();
        header.Columns.ForEach(x => columns.Add(x));
        header.Tables.ForEach(x => tables.Add(x));

        //version
        var vconfig = source.GetDestination().VersioningConfig;
        if (vconfig != null)
        {
            columns.Add($"__version.{vconfig.Sequence.Column}");
            tables.Add($"cross join ({source.ToSelectVersionSql()}) __version");
        }

        var selectcmd = new SelectCommand()
        {
            WithQuery = source.GetCteQuery(),
            Tables = tables,
            Columns = columns,
        };
        var prm = source.ToCreateTableParameter();
        if (prm != null) selectcmd.Parameters = prm;

        var sql = new CreateTableCommand(source.GetDatasource().BridgeName, selectcmd);
        sql.IsTemporary = true;
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static Dictionary<string, object>? ToCreateTableParameter(this IPier source)
    {
        var pier = source.CurrentPier;
        if (pier == null) return null;

        var current = pier.Filter?.ToParameters();
        var previous = pier.PreviousPrier?.ToCreateTableParameter();
        return current == null ? previous : current.Merge(previous);
    }

    private static string ToSelectVersionSql(this IPier source)
    {
        var config = source.GetDestination().VersioningConfig;
        if (config == null) throw new NotSupportedException($"versioning not supported.(table:{source.GetDestination().TableName})");
        var sql = $"select {config.Sequence.Command} as {config.Sequence.Column}";
        return sql;
    }

    private static (List<string> Columns, List<string> Tables) ToHeaderJoinInfo(this IPier source)
    {
        var dest = source.GetDestination();

        var columns = new List<string>();
        var tables = new List<string>();

        foreach (var item in dest.Groups)
        {
            var alias = item.GetInnerAlias();
            var cols = item.GetColumnsWithoutKey();
            var sql = @$"left join (
    select
        h.*
        , {item.Sequence.Command} as {item.Sequence.Column}
    from
        (
            select distinct {cols.ToString(", ")} from {source.ViewOrCteName}
        ) h
    ) {alias} on {cols.Select(x => $"{source.GetInnerAlias()}.{x} = {alias}.{x}").ToString(" and ")}";

            columns.Add($"{alias}.{item.Sequence.Column}");
            tables.Add(sql);
        }

        return (columns, tables);
    }
}