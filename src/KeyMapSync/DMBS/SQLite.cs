using KeyMapSync.Entity;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace KeyMapSync.DBMS;

public class SQLite : IDBMS
{
    private static int TableNameMaxLength => 128;

    public string ToKeyMapDDL(Datasource ds)
    {
        var tbl = ds.KeyMapName.RemoveOrDefault(TableNameMaxLength);

        var cols = ds.GetKeyMapColumns().Select(x => $"{x} integer not null").ToList();
        cols.Add($"primary key({ds.KeyColumns.ToString(", ")})");
        var col = cols.ToString("\r\n, ");
        var sql = $@"create table if not exists {tbl}
(
{col.AddIndent(4)}
)";

        return sql;
    }

    public string ToSyncDDL(Datasource ds)
    {
        var dest = ds.Destination;
        var tbl = dest.SyncName.RemoveOrDefault(TableNameMaxLength);
        var cols = dest.GetSyncColumns().Select(x => $"{x} integer not null").ToList();
        cols.Add($"primary key({ds.Destination.SequenceKeyColumn})");
        var col = cols.ToString("\r\n, ");
        var sql = $@"create table if not exists {tbl}
(
{col.AddIndent(4)}
)";

        return sql;
    }

    public string ToVersionDDL(Datasource ds)
    {
        var dest = ds.Destination;
        var tbl = dest.VersionName.RemoveOrDefault(TableNameMaxLength);
        var cols = new List<string>();
        cols.Add($"{dest.VersionKeyColumn} integer primary key autoincrement");
        cols.Add($"{dest.NameColumn} text not null");
        cols.Add($"{dest.TimestampColumn} timestamp not null default current_timestamp");
        //cols.Add($"primary key({dest.VersionKeyColumn})");
        var col = cols.ToString("\r\n, ");
        var sql = $@"create table if not exists {tbl}
(
{col.AddIndent(4)}
)";

        return sql;
    }

    public string ToOffsetDDL(Datasource ds)
    {
        var dest = ds.Destination;
        var tbl = dest.OffsetName.RemoveOrDefault(TableNameMaxLength);
        var cols = new List<string>();
        cols.Add($"{dest.SequenceKeyColumn} integer not null");
        cols.Add($"{dest.OffsetColumnName} integer not null");
        cols.Add($"{dest.RenewalColumnName} integer");
        cols.Add($"{dest.RemarksColumn} text not null");
        cols.Add($"primary key({dest.SequenceKeyColumn})");
        cols.Add($"unique({dest.OffsetColumnName})");
        cols.Add($"unique({dest.RenewalColumnName})");
        var col = cols.ToString("\r\n, ");
        var sql = $@"create table if not exists {tbl}
(
{col.AddIndent(4)}
)";

        return sql;
    }
}
