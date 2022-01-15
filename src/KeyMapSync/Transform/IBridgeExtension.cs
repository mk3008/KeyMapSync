using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public static class IBridgeExtension
{
    public static string GetBridgeName(this IBridge source)
    {
        return source.GetAbutment().BridgeName;
    }

    public static Datasource GetDatasource(this IBridge source)
    {
        return source.GetAbutment().Datasource;
    }

    public static Destination GetDestination(this IBridge source)
    {
        return source.GetAbutment().Datasource.Destination;
    }

    private static string ToSelectVersionSql(this IBridge source)
    {
        var dest = source.GetDestination();
        var sql = $"select {dest.VersionSequenceCommand} as {dest.VersionKeyColumn}";
        return sql;
    }

    public static string ToTemporaryDdl(this IPier source, bool isTemporary = true)
    {
        var bridgeName = source.GetBridgeName();
        //var pier = source.GetCurrentPier();
        //if (pier == null) throw new InvalidOperationException(nameof(pier));

        var command = isTemporary ? "create temporary table" : "create table";
        command = $"{command} {bridgeName}\r\nas";

        var withQuery = source.GetWithQuery();
        withQuery = (withQuery == null) ? null : $"\r\n{withQuery}"; ;

        var versionKey = source.GetDestination().VersionKeyColumn;
        var versionQuery = source.ToSelectVersionSql();

        var sql = $@"{command}{withQuery}
select
    __v.{versionKey}
    , {source.InnerAlias}.*
from {source.Name} {source.InnerAlias}
cross join ({versionQuery}) __v;";

        return sql;
    }

    private static string CreateInsertSql(string toTable, IList<string> toColumns, string fromTable, IList<string> fromColumns = null, bool useDistinct = false, string withQuery = null)
    {
        fromColumns ??= toColumns;

        var toCols = toColumns.ToString(", ");
        var fromCols = fromColumns.ToString(", ");
        var distinct = useDistinct ? " distinct" : "";
        var with = (withQuery == null) ? null : $"\r\n{withQuery}";

        var sql = $@"insert into {toTable} (
    {toCols}
){with}
select{distinct}
    {fromCols}
from
    {fromTable};";
        return sql;
    }

    public static ExpandoObject ToTemporaryParameter(this IBridge source)
    {
        var pier = source.GetCurrentPier();
        if (pier == null) return null;

        var current = pier.Filter?.ToParameter();
        var previous = pier.PreviousPrier?.ToTemporaryParameter();
        return current == null ? previous : current.Merge(previous);
    }

    public static string ToDestinationSql(this IBridge source)
    {
        if (source is ChangedPier) return ToDestinationSql_Change(source);

        var ds = source.GetDatasource();
        var dest = source.GetDestination();

        var toTable = dest.DestinationName;
        var fromTable = source.GetBridgeName();
        var cols = dest.Columns.Where(x => dest.SequenceKeyColumn != x).Where(x => ds.Columns.Contains(x)).ToList();

        var sql = CreateInsertSql(toTable, cols, fromTable);
        return sql;
    }

    public static string ToDestinationSql_Change(this IBridge source)
    {
        var ds = source.GetDatasource();
        var dest = source.GetDestination();

        var toTable = dest.DestinationName;
        var fromTable = source.GetBridgeName();
        var cols = dest.Columns.Where(x => dest.SequenceKeyColumn != x).Where(x => ds.Columns.Contains(x)).ToList();

        var sql = CreateInsertSql(toTable, cols, fromTable);
        return sql;
    }

    public static string ToKeyMapSql(this IBridge source)
    {
        var ds = source.GetDatasource();

        var toTable = ds.KeyMapName;
        var fromTable = source.GetBridgeName();
        var cols = ds.GetKeyMapColumns();

        var sql = CreateInsertSql(toTable, cols, fromTable);
        return sql;
    }

    public static string ToRemoveKeyMapSql(this IBridge source)
    {
        var ds = source.GetDatasource();
        var dest = source.GetDestination();

        var toTable = ds.KeyMapName;
        var fromTable = source.GetBridgeName();
        var key = dest.SequenceKeyColumn;

        var sql = $@"delete from {toTable} m
where
    exists (select * from {fromTable} t where m.{key} = t.{key});";
        return sql;
    }

    public static string ToSyncSql(this IBridge source)
    {
        var dest = source.GetDestination();

        var toTable = dest.SyncName;
        var fromTable = source.GetBridgeName();
        var cols = dest.GetSyncColumns();

        var sql = CreateInsertSql(toTable, cols, fromTable);
        return sql;
    }

    public static string ToVersionSql(this IBridge source)
    {
        var dest = source.GetDestination();

        var toTable = dest.VersionName;
        var fromTable = source.GetBridgeName();

        var cols = new List<string>();
        cols.Add(dest.VersionKeyColumn);
        cols.Add(dest.NameColumn);

        var vals = new List<string>();
        vals.Add(dest.VersionKeyColumn);
        vals.Add(":name");

        var sql = CreateInsertSql(toTable, cols, fromTable, vals, useDistinct: true);
        return sql;
    }

    public static ExpandoObject ToVersionParameter(this IBridge source)
    {
        var ds = source.GetDatasource();
        dynamic prm = new ExpandoObject();
        prm.name = ds.Name;
        return prm;
    }

    public static IList<string> ToExtensionSqls(this IBridge source)
    {
        var lst = new List<string>();

        var ds = source.GetDatasource();
        var bridgeName = source.GetBridgeName();

        foreach (var item in ds.Extensions)
        {
            var exDest = item.Destination;

            var toTable = exDest.DestinationName;
            var fromTable = item.AliasName;

            var cols = exDest.Columns;
            var wsql = string.Format(item.WithQueryFormat, bridgeName);

            var sql = CreateInsertSql(toTable, cols, fromTable, withQuery: wsql);
            lst.Add(sql);
        }

        return lst;
    }
}