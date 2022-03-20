using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace KeyMapSync.DBMS;

public class SQLite : IDBMS
{
    private static int TableNameMaxLength => 128;

    public string ToCreateTableSql(DbTable tbl)
    {
        var types = new Dictionary<DbColumn.Types, string>();
        types[DbColumn.Types.Numeric] = " integer";
        types[DbColumn.Types.Text] = " text";
        types[DbColumn.Types.Timestamp] = " timestamp";

        var nulls = new Dictionary<bool, string>();
        nulls[true] = "";
        nulls[false] = " not null";

        var seqs = new Dictionary<bool, string>();
        seqs[true] = " autoincrement";
        seqs[false] = "";

        var defs = new Dictionary<DbColumn.Types, string>();
        defs[DbColumn.Types.Numeric] = "";
        defs[DbColumn.Types.Text] = "";
        defs[DbColumn.Types.Timestamp] = " default current_timestamp";

        var cols = tbl.DbColumns.Select(x =>
        {
            if (x.Column == tbl.Sequence?.Column)
            {
                return $"{x.Column}{types[x.ColumnType]} primary key autoincrement";
            }
            else
            {
                return $"{x.Column}{types[x.ColumnType]}{nulls[x.IsNullable]}{seqs[x.Column == tbl.Sequence?.Column]}{defs[x.ColumnType]}";
            }
        }).ToList();

        if (tbl.Sequence == null) cols.Add($"primary key({tbl.Primarykeys.ToString(", ")})");
        tbl.UniqueKeyGroups.ForEach(keys => cols.Add($"unique({keys.ToString(", ")})"));

        var sql = $@"create table if not exists {tbl.Table}
(
{cols.ToString("\r\n, ").AddIndent(4)}
)";

        return sql;
    }
}
