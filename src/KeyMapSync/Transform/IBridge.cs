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

    public static string ToSql(this IBridge source, bool isTemporary = true )
    {
        var ext = source.BuildExtendWithQuery();
        if (ext != null) ext = $@",
{ext}";

        var command = isTemporary ? "create temporary table" : "create table";

        var sql = $@"{command} {source.BridgeName}
as
{source.GetWithQuery()}{ext}
select
    *
from {source.Alias};";
        return sql;
    }

    public static ExpandoObject ToParameter(this IBridge source)
    {
        var current = source.Filter?.ToParameter();
        var cascade = source.Owner?.ToParameter();
        return current == null ? cascade : current.Merge(cascade);
    }
}

