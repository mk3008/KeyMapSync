﻿using System.Collections.Generic;
using System.Data;
using System.Text;

namespace KeyMapSync
{
    public class PostgresDB : IDBMS
    {
        public int TableNameMaxLength => 63;

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
    and table_name = :table_name;";
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
    ordinal_position;";

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
    1;";

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
    and c.column_name = :column_name;";
            return new SqlEventArgs(sql, new { schema_name = info.SchemaName, table_name = info.TableName, column_name = columnName });
        }

        public string GetSequenceNextValueCommand(string sequenceName)
        {
            return $"nextval('{sequenceName}')";
        }

        public SqlEventArgs GetCreateVersionTableDDL(string tableName, string sequenceColumnName, string datasourceColumnName, string mappingColumnName)
        {
            var sql = 
$@"create table {tableName}
(
{sequenceColumnName} serial8 primary key
, {datasourceColumnName} text not null
, {mappingColumnName} text not null
, create_timestamp timestamp not null default current_timestamp
);
create index on {tableName}({datasourceColumnName});"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetCreateSyncTableDDL(string tableName, Table dest, Table version)
        {
            var sql = $@"
create table {tableName}
(
{dest.SequenceColumn.ColumnName} int8 primary key
, {version.SequenceColumn.ColumnName} int8 not null
, create_date_time timestamp not null default current_timestamp
);
create index on {tableName}({version.SequenceColumn.ColumnName});
"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetCreateKeymapTableDDL(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns)
        {
            var sql = $@"
create table {tableName}
(
{datasourceKeyColumns.ToString(",", x => $"{x} int8 not null")}
, {dest.SequenceColumn.ColumnName} int8 unique
, create_date_time timestamp not null default current_timestamp
, primary key({datasourceKeyColumns.ToString(",")})
)"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetCreateOffsetmapTableDDL(string tableName, Table dest, string offsetsourcePrefix, string offsetcomment)
        {
            var sql = $@"
create table {tableName}
(
{dest.SequenceColumn.ColumnName} int8 not null unique
, {offsetsourcePrefix}{dest.SequenceColumn.ColumnName} int8 not null unique
, {offsetcomment} text
, create_date_time timestamp not null default current_timestamp
)";
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetInsertVersionTableScalar(SyncMap def)
        {
            var version = def.KeyMap.VersionTable;

            var sql = $"insert into {version.TableFullName}(datasource_name, mapping_name) values (:datasource_name, :mapping_name) returning {version.SequenceColumn.ColumnName};";

            var prm = new
            {
                datasource_name = (def?.DatasourceName == null) ? "" : def.DatasourceName,
                mapping_name = (def?.KeyMap?.MappingName == null) ? "" : def.KeyMap.MappingName,
            };
            return new SqlEventArgs(sql, prm);
        }
    }
}