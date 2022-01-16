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
    public static (IList<string> fromCols, IList<string> toCols, string where) GetInsertDestinationInfo(this Destination source, string prefix = null)
    {
        var pre = (string.IsNullOrEmpty(prefix) ? null : $"{prefix}_");
        var vals = source.Columns.Where(x => source.SequenceKeyColumn != x).Where(x => source.Columns.Contains(x)).ToList();

        var fromCols = new List<string>();
        fromCols.Add($"{pre}{source.SequenceKeyColumn}");
        fromCols.AddRange(vals);

        var toCols = new List<string>();
        toCols.Add(source.SequenceKeyColumn);
        toCols.AddRange(vals);
        
        var where = (pre == null) ? null : $"where {pre}{source.SequenceKeyColumn} is not null";
        
        return (fromCols, toCols, where);
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

    public static (IList<string> fromCols, IList<string> toCols, string where) GetInsertSyncInfoset(this Destination source, string prefix = null)
    {
        var pre = (string.IsNullOrEmpty(prefix)) ? null : $"{prefix}_";

        var fromCols = new List<string>();
        fromCols.Add($"{pre}{source.SequenceKeyColumn}");
        fromCols.Add(source.VersionKeyColumn);

        var toCols = new List<string>();
        toCols.Add(source.SequenceKeyColumn);
        toCols.Add(source.VersionKeyColumn);

        var where = (pre == null) ? null : $"where {pre}{source.SequenceKeyColumn} is not null";

        return (fromCols, toCols, where);
    }
}
