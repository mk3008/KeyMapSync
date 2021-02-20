using System.Collections.Generic;
using System.Data;
using System.Text;

namespace KeyMapSync
{
    public class PostgresDB : IDBMS
    {
        public SqlEventArgs GetFindTableNameInfoQuery(string name)
        {
            var info = name.ToTableInfo();
            return GetFindTableNameInfoQuery(info.SchemaName, info.TableName);
        }

        public SqlEventArgs GetFindTableNameInfoQuery(string schemaName, string tableName)
        {
            var sql = @"
select
    table_schema as schemaname
    , table_name as tablename
from
    information_schema.tables
where
    (:schema_name = '' or table_schema = :schema_name)
    and table_name = :table_name
";
            return new SqlEventArgs(sql, new { schema_name = schemaName, table_name = tableName });
        }

        public SqlEventArgs GetColumnsSql(TableNameInfo info)
        {
            var sql = @"
select
    column_name
from
    information_schema.columns
where
    table_schema = :schema_name
    and table_name = :table_name
order by
    ordinal_position";

            return new SqlEventArgs(sql, new { schema_name = info.SchemaName, table_name = info.TableName });
        }

        public SqlEventArgs GetSequenceColumnScalar(TableNameInfo info)
        {
            var sql = @"
select
    column_name
from
    information_schema.columns
where
    table_schema = :schema_name
    and table_name = :table_name
    and column_default like 'nextval(''%'
order by
    ordinal_position
limit
    1
";

            return new SqlEventArgs(sql, new { schema_name = info.SchemaName, table_name = info.TableName });
        }

        public SqlEventArgs GetSequenceNameScalar(IDbConnection cn, TableNameInfo info, string columnName)
        {
            var sql = @"
select
    replace(substring(substring(c.column_default from '^nextval\(\''.*\''') from '\''.*\''' ) , '''', '') as sequence_name 
from
    information_schema.columns c
    inner join information_schema.tables t on c.table_catalog = t.table_catalog and c.table_schema = t.table_schema and c.table_name = t.table_name
where
    t.table_schema = :schema_name
    and t.table_name = :table_name
    and c.column_name = :column_name
";
            return new SqlEventArgs(sql, new { schema_name = info.SchemaName, table_name = info.TableName, column_name = columnName });
        }

        public string GetSequenceNextValueCommand(string sequenceName)
        {
            return $"nextval('{sequenceName}')";
        }

        public SqlEventArgs GetCreateSyncVersionTableDDL(string tableName, string sequenceColumnName)
        {
            var sql = @$"
create table {tableName}
(
{sequenceColumnName} serial8 primary key
, mapping_name text not null
, create_timestamp timestamp not null default current_timestamp
)"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetCreateSyncTableDDL(string tableName, Table dest, Table version)
        {
            var sql = @$"
create table {tableName}
(
{dest.SequenceColumn.ColumnName} int8 primary key
, {version.SequenceColumn.ColumnName} int8 not null
)"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetCreateMappingTableDDL(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns)
        {
            var sql = @$"
create table {tableName}
(
{datasourceKeyColumns.ToString(",", x => $"{x} int8 not null")}
, {dest.SequenceColumn.ColumnName} int8 unique
, primary key({datasourceKeyColumns.ToString(",")})
)"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetInsertVersionTableScalar(SyncMap def)
        {
            var version = def.VersionTable;

            var sql = $"insert into {version.TableFullName}(mapping_name) values (:mapping_name) returning {version.SequenceColumn.ColumnName};";
            var prm = new { mapping_name = def.MappingName };
            return new SqlEventArgs(sql, prm);
        }
    }
}