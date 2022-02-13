using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal static class DatasourceExtension
{
    public static IList<string> GetKeyMapColumns(this Datasource source)
    {
        var lst = new List<string>();
        lst.Add(source.Destination.SequenceKeyColumn);
        lst.AddRange(source.KeyColumns);
        return lst;
    }

    public static (IList<string> fromColumns, IList<string> toColumns, string? where) GetInsertKeyMapInfoset(this Datasource source, string? prefix = null)
    {
        var key = source.Destination.SequenceKeyColumn;

        var fromCols = new List<string>();
        fromCols.Add($"{prefix}{key}");
        fromCols.AddRange(source.KeyColumns);

        var toCols = new List<string>();
        toCols.Add(key);
        toCols.AddRange(source.KeyColumns);

        var where = (string.IsNullOrEmpty(prefix)) ? null : $"where {prefix}{key} is not null";

        return (fromCols, toCols, where);
    }
}
