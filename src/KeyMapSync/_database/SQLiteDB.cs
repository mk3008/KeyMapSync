using System.Collections.Generic;
using System.Data;
using System.Text;

namespace KeyMapSync
{
    public class SQLiteDB : IDBMS
    {
        public int TableNameMaxLength => 128;

        public SqlEventArgs GetFindTableNameInfoQuery(string name)
        {
            return GetFindTableNameInfoQuery(string.Empty, name);
        }

        public SqlEventArgs GetFindTableNameInfoQuery(string schemaName, string tableName)
        {
            var sql = @"
select '' as schemaname ,tbl_name as tablename from sqlite_master where type='table' and tbl_name = :table_name
union all
select '' as schemaname ,tbl_name as tablename from sqlite_temp_master where type='table' and tbl_name = :table_name
";

            return new SqlEventArgs(sql, new { table_name = tableName });
        }

        public SqlEventArgs GetColumnsSql(TableNameInfo info)
        {
            var sql = "select name as column_name from pragma_table_info(:table_name) order by cid";
            return new SqlEventArgs(sql, new { table_name = info.TableName });
        }

        public SqlEventArgs GetSequenceColumnScalar(TableNameInfo info)
        {
            var sql = "select name as column_name from pragma_table_info(:table_name) where pk = 1 order by cid limit 1";
            return new SqlEventArgs(sql, new { table_name = info.TableName });
        }

        public SqlEventArgs GetSequenceNameScalar(IDbConnection cn, TableNameInfo info, string columnName)
        {
            var sql = "select '(select max(seq) from (select seq from sqlite_sequence where name = ''' || :table_name || ''' union all select 0))'";
            return new SqlEventArgs(sql, new { table_name = info.TableName });
        }

        public string GetSequenceNextValueCommand(string sequenceName)
        {
            return $"row_number() over() + {sequenceName}";
        }

        public SqlEventArgs GetCreateSyncVersionTableDDL(string tableName, string sequenceColumnName)
        {
            var sql = @$"
create table {tableName}
(
{sequenceColumnName} integer primary key autoincrement
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
{dest.SequenceColumn.ColumnName} integer primary key
, {version.SequenceColumn.ColumnName} integer not null
)"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetCreateMappingTableDDL(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns)
        {
            var sql = @$"
create table {tableName}
(
{datasourceKeyColumns.ToString(",", x => $"{x} integer not null")}
, {dest.SequenceColumn.ColumnName} integer unique
, primary key({datasourceKeyColumns.ToString(",")})
)"
;
            return new SqlEventArgs(sql);
        }

        public SqlEventArgs GetInsertVersionTableScalar(SyncMap def)
        {
            var version = def.VersionTable;

            var sql = $"insert into {version.TableFullName}(mapping_name) values (:mapping_name); select last_insert_rowid();";
            var prm = new { mapping_name = def.MappingName };
            return new SqlEventArgs(sql, prm);
        }
    }
}