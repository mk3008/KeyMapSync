using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace KeyMapSync
{
    public class DbExecutor
    {
        public static event EventHandler<SqlEventArgs> OnBeforeExecute;

        public int? CommandTimeout { get; set; }

        public DbExecutor(IDBMS db, IDbConnection cn)
        {
            DB = db;
            Connection = cn;
        }

        public IDBMS DB { get; private set; }

        public IDbConnection Connection { get; private set; }

        private TableNameInfo ReadTableNameInfo(string tableName)
        {
            var args = DB.GetFindTableNameInfoQuery(tableName);
            OnBeforeExecute?.Invoke(this, args);
            var lst = Connection.Query<TableNameInfo>(args.Sql, args.Param, commandTimeout: CommandTimeout).AsList();

            if (lst.Any() == false)
            {
                return null;
            }
            else if (lst.Count == 1)
            {
                return lst.First();
            }
            throw new ArgumentException("Multiple tables found.");
        }

        public string ReadSequenceName(TableNameInfo info, string sequenceColumn)
        {
            var args = DB.GetSequenceNameScalar(Connection, info, sequenceColumn);
            OnBeforeExecute?.Invoke(this, args);
            return (string)Connection.ExecuteScalar(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public IEnumerable<string> ReadColumns(TableNameInfo info)
        {
            var args = DB.GetColumnsSql(info);
            OnBeforeExecute?.Invoke(this, args);
            return Connection.Query<string>(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public string ReadSequenceColumn(TableNameInfo info)
        {
            var args = DB.GetSequenceColumnScalar(info);
            OnBeforeExecute?.Invoke(this, args);
            return Connection.ExecuteScalar<string>(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public void CreateSyncVersionTable(string tableName, string sequenceColumnName)
        {
            var args = DB.GetCreateSyncVersionTableDDL(tableName, sequenceColumnName);
            OnBeforeExecute?.Invoke(this, args);
            Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public void CreateSyncTable(string tableName, Table dest, Table version)
        {
            var args = DB.GetCreateSyncTableDDL(tableName, dest, version);
            OnBeforeExecute?.Invoke(this, args);
            Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public void CreateMappingTable(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns)
        {
            var args = DB.GetCreateMappingTableDDL(tableName, dest, datasourceKeyColumns);
            OnBeforeExecute?.Invoke(this, args);
            Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public Table ReadTable(string tableName)
        {
            var info = ReadTableNameInfo(tableName);
            if (info == null) return null;

            var table = new Table(info);
            var column = ReadSequenceColumn(info);
            var name = ReadSequenceName(info, column);
            var command = DB.GetSequenceNextValueCommand(name);

            table.SequenceColumn = new SequenceColumn() { ColumnName = column, NextValCommand = command };
            table.Columns = ReadColumns(info).ToList();
            return table;
        }

        public Table ReadMappingTableInfo(string tableName)
        {
            var info = ReadTableNameInfo(tableName);
            if (info == null) return null;

            var table = new Table(info);
            table.Columns = ReadColumns(info).ToList();
            return table;
        }

        /// <summary>
        /// create temporary table.
        /// </summary>
        /// <param name="def"></param>
        public int CreateTemporay(SyncMap def)
        {
            var tmp = def.TemporaryTable;
            var map = def.MappingTable;

            var where = !def.IsNeedExistsCheck ? "" : $"where not exists (select * from {map.TableFullName} x where {tmp.SourceKeyColumns.ToString(" and ", x => $"x.{x} = {tmp.DatasourceAliasName}.{x}")})";

            var sql = @$"
create temporary table {tmp.TableName}
as
{tmp.DatasourceQuery}
select
    {tmp.DestinationSequence.NextValCommand} as {tmp.DestinationSequence.ColumnName}, {tmp.DatasourceAliasName}.*
from
    {tmp.DatasourceAliasName}
{where}
order by
    {tmp.SourceKeyColumns.ToString(", ", x => $"{tmp.DatasourceAliasName}.{x}")}
";
            var param = tmp.ParamGenerator?.Invoke();
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, param));
            Connection.Execute(sql, param, commandTimeout: CommandTimeout);

            //retrun insert count
            var cntSql = $"select count(*) from {tmp.TableName}";
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(cntSql));

            return Connection.ExecuteScalar<int>(cntSql, commandTimeout: CommandTimeout);
        }

        /// <summary>
        /// inesrt ioto destination table.
        /// </summary>
        /// <param name="trn"></param>
        /// <param name="tmp"></param>
        /// <param name="dest"></param>
        public int InsertDestinationTable(SyncMap def)
        {
            var dest = def.DestinationTable;
            var tmp = ReadTable(def.TemporaryTable.TableName);

            var sql = @$"
insert into {dest.TableFullName}(
{dest.Columns.Where(x => tmp.Columns.Contains(x)).ToString(",")}
)
select
{dest.Columns.Where(x => tmp.Columns.Contains(x)).ToString(",")}
from
{tmp.TableName}
order by
{dest.SequenceColumn.ColumnName}
; ";

            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
            return Connection.Execute(sql, commandTimeout: CommandTimeout);
        }

        public long InsertVersionTable(SyncMap def)
        {
            var args = DB.GetInsertVersionTableScalar(def);
            OnBeforeExecute?.Invoke(this, args);
            return (long)Connection.ExecuteScalar(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        /// <summary>
        /// insert into sync table
        /// </summary>
        /// <param name="def"></param>
        /// <param name="versionNo"></param>
        public int InsertSyncTable(SyncMap def, long versionNo)
        {
            var sync = def.SyncTable;
            var version = def.VersionTable;
            var tmp = def.TemporaryTable;
            var columnsSql = new StringBuilder();

            foreach (var item in sync.Columns)
            {
                if (columnsSql.Length != 0)
                {
                    columnsSql.Append(", ");
                }

                if (item == version.SequenceColumn.ColumnName)
                {
                    columnsSql.Append($":version_no as {item}");
                }
                else
                {
                    columnsSql.Append($"{item}");
                }
            }

            var sql = @$"
insert into {sync.TableFullName}
select distinct
    {columnsSql}
from
    {tmp.TableName}
;
";

            var prm = new { version_no = versionNo };
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
            return Connection.Execute(sql, prm, commandTimeout: CommandTimeout);
        }

        /// <summary>
        /// insert into map table.
        /// </summary>
        /// <param name="def"></param>
        public int InsertMappingTable(SyncMap def)
        {
            var map = def.MappingTable;
            var tmp = def.TemporaryTable;

            var sql = @$"
insert into {map.TableFullName}
select
    {map.Columns.ToString(",")}
from
    {tmp.TableName}
order by
    {map.Columns.ToString(",")}
;
";
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
            return Connection.Execute(sql, commandTimeout: CommandTimeout);
        }
    }
}