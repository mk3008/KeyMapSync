using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class Postgres : IDBMS
{
    private static int TableNameMaxLength => 128;

    public string ConcatFunctionToken => "concat";

    public string ConcatSplitToken => ", ";

    public string ToCreateTableSql(DbTable tbl)
    {
        var types = new Dictionary<DbColumn.Types, string>();
        types[DbColumn.Types.Numeric] = " int8";
        types[DbColumn.Types.Text] = " text";
        types[DbColumn.Types.Timestamp] = " timestamp";

        var nulls = new Dictionary<bool, string>();
        nulls[true] = "";
        nulls[false] = " not null";

        var seqs = new Dictionary<bool, string>();
        seqs[true] = " serial8";
        seqs[false] = "";

        var defs = new Dictionary<DbColumn.Types, string>();
        defs[DbColumn.Types.Numeric] = "";
        defs[DbColumn.Types.Text] = "";
        defs[DbColumn.Types.Timestamp] = " default current_timestamp";

        var cols = tbl.DbColumns.Select(x =>
        {
            if (x.Column == tbl.Sequence?.Column)
            {
                return $"{x.Column}{seqs[true]} primary key";
            }
            else
            {
                return $"{x.Column}{types[x.ColumnType]}{seqs[false]} {nulls[x.IsNullable]}{defs[x.ColumnType]}";
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