using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public interface IBridge
{
    Datasource Datasource { get; }

    IBridge Owner { get; }

    BridgeRoot GetRoot();

    string GetWithQuery();

    string BuildExtendWithQuery();

    /// <summary>
    /// ex."__ds"
    /// </summary>
    string Alias { get; }

    /// <summary>
    /// ex."tmp01"
    /// </summary>
    string BridgeName { get; }

    IFilter Filter { get; }
}

public static class IBridgeExtension
{
    public static string GetDatasourceAlias(this IBridge source)
    {
        if (source.Owner == null || source is BridgeRoot || source is FilterBridge) return source.Alias;
        return source.Owner.GetDatasourceAlias();
    }

    public static string GetInnerDatasourceAlias(this IBridge source)
    {
        return "__ds";
    }

    public static string ToTemporaryDdl(this IBridge source, bool isTemporary = true)
    {
        //var ext = source.BuildExtendWithQuery();
        //if (ext != null) ext = $",\r\n{ext}";

        var command = isTemporary ? "create temporary table" : "create table";
        var w = source.GetWithQuery();
        w = (w == null) ? null : $"{w}\r\n"; ;
        var dest = source.Datasource.Destination;
        var sql = $@"{command} {source.BridgeName}
as
{w}select
    __v.{dest.VersionKeyColumn}
    , {source.GetInnerDatasourceAlias()}.*
from {source.Alias} {source.GetInnerDatasourceAlias()}
cross join (select {dest.VersionSequenceCommand} as {dest.VersionKeyColumn}) __v;";
        return sql;
    }

    public static ExpandoObject ToTemporaryParameter(this IBridge source)
    {
        var current = source.Filter?.ToParameter();
        var cascade = source.Owner?.ToTemporaryParameter();
        return current == null ? cascade : current.Merge(cascade);
    }

    public static string ToDestinationSql(this IBridge source)
    {
        if (source is ChangedBridge) return ToDestinationSql_Change(source);
        var ds = source.Datasource;
        var dest = source.Datasource.Destination;
        var toTable = dest.DestinationName;
        var fromTable = source.BridgeName;
        var col = dest.Columns.Where(x => dest.SequenceKeyColumn != x).Where(x => ds.Columns.Contains(x)).ToString(", ");
        var sql = $@"insert into {toTable} (
    {col}
)
select
    {col}
from
    {fromTable};";
        return sql;
    }

    public static string ToDestinationSql_Change(this IBridge source)
    {
        var ds = source.Datasource;
        var dest = source.Datasource.Destination;
        var toTable = dest.DestinationName;
        var fromTable = source.BridgeName;
        var col = dest.Columns.Where(x => dest.SequenceKeyColumn != x).Where(x => ds.Columns.Contains(x)).ToString(", ");
        var sql = $@"insert into {toTable} (
    {col}
)
select
    {col}
from
    {fromTable};";
        return sql;
    }

    public static string ToKeyMapSql(this IBridge source)
    {
        var ds = source.Datasource;
        var dest = source.Datasource.Destination;
        var toTable = ds.KeyMapName;
        var fromTable = source.BridgeName;
        var col = ds.GetKeyMapColumns().ToString(", ");
        var sql = $@"insert into {toTable} (
    {col}
)
select
    {col}
from
    {fromTable};";
        return sql;
    }

    public static string ToRemoveKeyMapSql(this IBridge source)
    {
        var ds = source.Datasource;
        var dest = source.Datasource.Destination;
        var toTable = ds.KeyMapName;
        var fromTable = source.BridgeName;
        var col = dest.SequenceKeyColumn;
        var sql = $@"delete from {toTable} m
where
    exists (select * from {fromTable} t where m.{col} = t.{col});";
        return sql;
    }

    public static string ToSyncSql(this IBridge source)
    {
        var ds = source.Datasource;
        var dest = source.Datasource.Destination;
        var toTable = dest.SyncName;
        var fromTable = source.BridgeName;
        var col = dest.GetSyncColumns().ToString(", ");
        var sql = $@"insert into {toTable} (
    {col}
)
select
    {col}
from
    {fromTable};";
        return sql;
    }

    public static string ToVersionSql(this IBridge source)
    {
        var ds = source.Datasource;
        var dest = source.Datasource.Destination;
        var toTable = dest.VersionName;
        var fromTable = source.BridgeName;

        var cols = new List<string>();
        cols.Add(dest.VersionKeyColumn);
        cols.Add(dest.NameColumn);
        var col = cols.ToString(", ");

        var vals = new List<string>();
        vals.Add(dest.VersionKeyColumn);
        vals.Add(":name");
        var val = vals.ToString(", ");

        var sql = $@"insert into {toTable} (
    {col}
)
select distinct
    {val}
from
    {fromTable};";
        return sql;
    }

    public static ExpandoObject ToVersionParameter(this IBridge source)
    {
        var ds = source.Datasource;
        dynamic prm = new ExpandoObject();
        prm.name = ds.Name;
        return prm;
    }

    public static IList<string> ToExtensionSqls(this IBridge source)
    {
        var lst = new List<string>();

        var ds = source.Datasource;
        foreach (var item in ds.Extensions)
        {
            var dst = item.Destination;
            var toTable = dst.DestinationName;
            var fromTable = item.AliasName;
            var col = dst.Columns.ToString(", ");
            var wsql = string.Format(item.WithQueryFormat, source.BridgeName);

            var sql = $@"insert into {toTable}(
    {col}
)
{wsql}
select
    {col}
from
    {fromTable};";
            lst.Add(sql);
        }

        return lst;
    }
}

