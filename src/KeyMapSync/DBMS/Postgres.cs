using Dapper;
using KeyMapSync.Entity;
using SqModel;
using SqModel.Analysis;
using SqModel.Dapper;
using System.Data;
using System.Text.Json;
using System.Xml.Linq;

namespace KeyMapSync.DBMS;

public partial class Postgres : IDBMS
{
    public string ToCreateTableSql(DbTable tbl)
    {
        var types = new Dictionary<DbColumnType, string>();
        types[DbColumnType.Numeric] = " int8";
        types[DbColumnType.Text] = " text";
        types[DbColumnType.Timestamp] = " timestamp";

        var nulls = new Dictionary<bool, string>();
        nulls[true] = "";
        nulls[false] = " not null";

        var seqs = new Dictionary<bool, string>();
        seqs[true] = " serial8";
        seqs[false] = "";

        var defs = new Dictionary<DbColumnType, string>();
        defs[DbColumnType.Numeric] = "";
        defs[DbColumnType.Text] = "";
        defs[DbColumnType.Timestamp] = " default current_timestamp";

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
        if (tbl.UniqueKeyGroups.Any()) cols.Add($"unique({tbl.UniqueKeyGroups.ToString(", ")})");

        var sql = $@"create table if not exists {tbl.Table}
(
{cols.ToString("\r\n, ").AddIndent(4)}
)";

        return sql;
    }

    public string GetSequenceSql()
    {
        var sql = @"select
    u.column_name as column
    , col.column_default as command
from
    information_schema.table_constraints c
    inner join information_schema.constraint_column_usage u on c.table_catalog = u.table_catalog and c.table_schema = u.table_schema and c.table_name = u.table_name and c.constraint_name = u.constraint_name
    inner join information_schema.columns col on u.table_catalog = col.table_catalog and u.table_schema = col.table_schema and u.table_name = col.table_name and u.column_name = col.column_name
where
    (coalesce(:schema, '') = '' or c.table_schema = :schema)
    and c.table_name = :table
    and c.constraint_type = 'PRIMARY KEY'";

        return sql;
    }

    public string GetColumnsSql()
    {
        var sql = @"select
    c.column_name
from
    information_schema.columns c
where
    (coalesce(:schema, '') = '' or c.table_schema = :schema)
    and c.table_name = :table
order by 
    c.ordinal_position";

        return sql;
    }

    public string GetKeyColumnsSql()
    {
        var sql = @"select
    u.column_name
    , col.data_type 
from
    information_schema.table_constraints c
    inner join information_schema.constraint_column_usage u on c.table_catalog = u.table_catalog and c.table_schema = u.table_schema and c.table_name = u.table_name and c.constraint_name = u.constraint_name
    inner join information_schema.columns col on u.table_catalog = col.table_catalog and u.table_schema = col.table_schema and u.table_name = col.table_name and u.column_name = col.column_name
where
    (:schema is null or c.table_schema = :schema)
    and c.table_name = :table
    and c.constraint_type = 'PRIMARY KEY'";

        return sql;
        //var q = Connection.Query(sql, new { schema, table }).ToList();
        //var dic = new Dictionary<string, Types>();

        //q.ForEach(x =>
        //{
        //    if (x.data_type == "smallint") dic[x.column_name] = Types.Numeric;
        //    else if (x.data_type == "int2") dic[x.column_name] = Types.Numeric;

        //    else if (x.data_type == "integer") dic[x.column_name] = Types.Numeric;
        //    else if (x.data_type == "int") dic[x.column_name] = Types.Numeric;
        //    else if (x.data_type == "int4") dic[x.column_name] = Types.Numeric;

        //    else if (x.data_type == "bigint") dic[x.column_name] = Types.Numeric;
        //    else if (x.data_type == "int8") dic[x.column_name] = Types.Numeric;

        //    else if (x.data_type == "serial") dic[x.column_name] = Types.Numeric;
        //    else if (x.data_type == "serial4") dic[x.column_name] = Types.Numeric;

        //    else if (x.data_type == "bigserial") dic[x.column_name] = Types.Numeric;
        //    else if (x.data_type == "serial8") dic[x.column_name] = Types.Numeric;

        //    else if (x.data_type == "date") dic[x.column_name] = Types.Date;

        //    else if (x.data_type == "timestamp") dic[x.column_name] = Types.Timestamp;

        //    else dic[x.column_name] = Types.Text;
        //});

        //return dic;
    }
}