using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal static class DestinationExtension
{
    public static (IList<string> fromCols, IList<string> toCols, string? where) GetInsertDestinationInfo(this Destination source, string? prefix = null)
    {
        var vals = source.Columns.Where(x => source.SequenceKeyColumn != x).Where(x => source.Columns.Contains(x)).ToList();

        var fromCols = new List<string>();
        fromCols.Add($"{prefix}{source.SequenceKeyColumn}");
        fromCols.AddRange(vals);

        var toCols = new List<string>();
        toCols.Add(source.SequenceKeyColumn);
        toCols.AddRange(vals);

        var where = (string.IsNullOrEmpty(prefix)) ? null : $"where {prefix}{source.SequenceKeyColumn} is not null";

        return (fromCols, toCols, where);
    }

    public static (IList<string> fromColumns, IList<string> toColumns, string? where) GetReverseInsertDestinationInfo(this Destination source)
    {
        var lst = source.Columns.Where(x => source.SequenceKeyColumn != x).Where(x => source.Columns.Contains(x)).ToList();

        //to
        var toCols = new List<string>();
        toCols.Add(source.SequenceKeyColumn);
        toCols.AddRange(lst);

        //from
        var fromCols = new List<string>();
        fromCols.Add($"{source.OffsetColumnPrefix}{source.SequenceKeyColumn}");
        foreach (var item in lst)
        {
            if (source.SingInversionColumns.Contains(item))
            {
                fromCols.Add($"{item} * -1 as {item}");
            }
            else
            {
                fromCols.Add(item);
            }
        }
        return (fromCols, toCols, null);
    }

    /// <summary>
    /// Get sync-table column list.
    /// </summary>
    /// <returns></returns>
    public static IList<string> GetSyncColumns(this Destination source)
    {

        var lst = new List<string>();
        lst.Add(source.SequenceKeyColumn);
        lst.Add(source.VersionKeyColumn);

        return lst;
    }

    public static (IList<string> fromCols, IList<string> toCols, string? where) GetInsertSyncInfoset(this Destination source, string? prefix = null)
    {
        var fromCols = new List<string>();
        fromCols.Add($"{prefix}{source.SequenceKeyColumn}");
        fromCols.Add(source.VersionKeyColumn);

        var toCols = new List<string>();
        toCols.Add(source.SequenceKeyColumn);
        toCols.Add(source.VersionKeyColumn);

        var where = (string.IsNullOrEmpty(prefix)) ? null : $"where {prefix}{source.SequenceKeyColumn} is not null";

        return (fromCols, toCols, where);
    }

    public static IList<string> GetOffsetKeyMapColumns(this Destination source)
    {
        var cols = new List<string>();
        cols.Add(source.SequenceKeyColumn);
        cols.Add(source.OffsetColumnName);
        cols.Add(source.RenewalColumnName);
        cols.Add(source.OffsetRemarksColumn);
        return cols;
    }

    public static (IList<string> fromColumns, IList<string> toColumns, string? where) GetInsertOffsetKeyMapInfoset(this Destination source, string? prefix = null)
    {
        var toColumns = source.GetOffsetKeyMapColumns();

        var fromColumns = new List<string>();
        fromColumns.Add(source.SequenceKeyColumn);
        fromColumns.Add(source.OffsetColumnName);
        fromColumns.Add(source.RenewalColumnName);
        fromColumns.Add($"_{source.OffsetRemarksColumn}");
        return (fromColumns, toColumns, null);
    }
}
