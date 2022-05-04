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
    //public static (List<string> fromCols, List<string> toCols, string? where) GetInsertDestinationInfo(this Destination source, string? prefix = null)
    //{
    //    var fromCols = new List<string>();
    //    var toCols = new List<string>();

    //    var seq = source.Sequence;
    //    if (seq != null)
    //    {
    //        fromCols.Add($"{prefix}{seq.Column}");
    //        toCols.Add(seq.Column);
    //    }

    //    var vals = source.Columns.Where(x => seq?.Column != x).Where(x => source.Columns.Contains(x)).ToList();
    //    fromCols.AddRange(vals);
    //    toCols.AddRange(vals);

    //    var where = (string.IsNullOrEmpty(prefix) || seq == null) ? null : $"where {prefix}{seq.Column} is not null";
    //    return (fromCols, toCols, where);
    //}

    //public static (List<string> fromColumns, List<string> toColumns, string? where) GetReverseInsertDestinationInfo(this Destination source)
    //{
    //    var fromCols = new List<string>();
    //    var toCols = new List<string>();

    //    var seq = source.Sequence;
    //    if (seq != null)
    //    {
    //        fromCols.Add($"{source.OffsetColumnPrefix}{seq.Column}");
    //        toCols.Add(seq.Column);
    //    }

    //    var vals = source.Columns.Where(x => seq?.Column != x).Where(x => source.Columns.Contains(x)).ToList();

    //    //from
    //    foreach (var item in vals)
    //    {
    //        if (source.SingInversionColumns.Contains(item))
    //        {
    //            fromCols.Add($"{item} * -1 as {item}");
    //        }
    //        else
    //        {
    //            fromCols.Add(item);
    //        }
    //    }

    //    //to
    //    toCols.AddRange(vals);

    //    return (fromCols, toCols, null);
    //}

    ///// <summary>
    ///// Get sync-table column list.
    ///// </summary>
    ///// <returns></returns>
    //public static List<string> GetSyncColumns(this Destination source)
    //{
    //    var lst = new List<string>();

    //    var seq = source.Sequence;
    //    if (seq == null) return lst;

    //    lst.Add(seq.Column);
    //    lst.Add(source.VersionKeyColumn);

    //    return lst;
    //}

    //public static (List<string> fromCols, List<string> toCols, string? where) GetInsertSyncInfoset(this Destination source, string? prefix = null)
    //{
    //    var fromCols = new List<string>();
    //    var toCols = new List<string>();

    //    var seq = source.Sequence;
    //    if (seq == null) return (fromCols, toCols, null);

    //    fromCols.Add($"{prefix}{seq.Column}");
    //    fromCols.Add(source.VersionKeyColumn);

    //    toCols.Add(seq.Column);
    //    toCols.Add(source.VersionKeyColumn);

    //    var where = (string.IsNullOrEmpty(prefix) || seq == null) ? null : $"where {prefix}{seq.Column} is not null";

    //    return (fromCols, toCols, where);
    //}

    //public static List<string> GetOffsetKeyMapColumns(this Destination source)
    //{
    //    var cols = new List<string>();
        
    //    var seq = source.Sequence;
    //    if (seq == null ) return cols;

    //    cols.Add(seq.Column);
    //    cols.Add(source.OffsetColumnName);
    //    cols.Add(source.RenewalColumnName);
    //    cols.Add(source.OffsetRemarksColumn);
    //    return cols;
    //}

    //public static (List<string> fromColumns, List<string> toColumns, string? where) GetInsertOffsetKeyMapInfoset(this Destination source, string? prefix = null)
    //{
    //    var fromCols = new List<string>();
    //    var toCols = new List<string>();

    //    var seq = source.Sequence;
    //    if (seq == null) return (fromCols, toCols, null);

    //    fromCols.Add(seq.Column);
    //    fromCols.Add(source.OffsetColumnName);
    //    fromCols.Add(source.RenewalColumnName);
    //    fromCols.Add($"_{source.OffsetRemarksColumn}");

    //    toCols = source.GetOffsetKeyMapColumns();

    //    return (fromCols, toCols, null);
    //}
}
