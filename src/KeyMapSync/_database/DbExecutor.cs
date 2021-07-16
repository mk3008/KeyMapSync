using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace KeyMapSync
{
    public partial class DbExecutor
    {
        public static event EventHandler<SqlEventArgs> OnBeforeExecute;

        public static event EventHandler<SqlResultArgs> OnAfterExecute;

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

            if (column != null)
            {
                var name = ReadSequenceName(info, column);
                var command = DB.GetSequenceNextValueCommand(name);
                table.SequenceColumn = new SequenceColumn()
                {
                    ColumnName = column,
                    NextValCommand = command
                };
            }

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
        public int CreateTemporayOfDefault(SyncMap def)
        {
            if (def.DatasourceMap.IsExtension)
            {
                var s = $"{def.DatasourceMap.DatasourceQueryGenarator(def.Sender)} select count(*) from {def.DatasourceMap.DatasourceAliasName}";
                OnBeforeExecute?.Invoke(this, new SqlEventArgs(s));
                return Connection.ExecuteScalar<int>(s, commandTimeout: CommandTimeout);
            }

            var dsmap = def.DatasourceMap;
            var map = def.MappingTable;
            var dst = def.DestinationTable;

            var seq = (dst?.SequenceColumn == null || def.DatasourceMap.IsBridge) ? "" : $"{dst.SequenceColumn.NextValCommand} as { dst.SequenceColumn.ColumnName}, ";

            var orderText = (dsmap?.DatasourceKeyColumns == null) ? "" : $"order by {dsmap.DatasourceKeyColumns.ToString(", ", x => $"{dsmap.DatasourceAliasName}.{x}")}";

            var where = !def.IsNeedExistsCheck ? "" : $"where not exists (select * from {map.TableFullName} x where {dsmap.DatasourceKeyColumns.ToString(" and ", x => $"x.{x} = {dsmap.DatasourceAliasName}.{x}")})";

            var sql = $@"
--{def.BridgeChainName}
create temporary table {def.BridgeTableName}
as
{dsmap.DatasourceQueryGenarator(def.Sender)}
select
    {seq}{dsmap.DatasourceAliasName}.*
from
    {dsmap.DatasourceAliasName}
{where}
{orderText}
";
            ExpandoObject prm = dsmap.ParameterGenerator?.Invoke();
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
            Connection.Execute(sql, prm, commandTimeout: CommandTimeout);

            //retrun insert count
            var cntSql = $"select count(*) from {def.BridgeTableName}";
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(cntSql));

            var cnt = Connection.ExecuteScalar<int>(cntSql, commandTimeout: CommandTimeout);
            OnAfterExecute?.Invoke(this, new SqlResultArgs(sql, cnt, prm) { TableName = def.BridgeTableName });

            return cnt;
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
            if (dest == null || dest.TableFullName == null) return 0;

            if (def.DatasourceMap.IsExtension)
            {
                var tmp = ReadTable(def.BridgeTableName);
                var orderSql = dest.SequenceColumn == null ? "" : $"order by {dest.SequenceColumn.ColumnName}";

                var sql = $@"
insert into {dest.TableFullName}(
{dest.Columns.Where(x => tmp.Columns.Contains(x)).ToString(",")}
)
{def.DatasourceMap.DatasourceQueryGenarator(def.Sender)}
select
{dest.Columns.Where(x => tmp.Columns.Contains(x)).ToString(",")}
from
{def.DatasourceMap.DatasourceAliasName}
{orderSql}
;";

                OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
                var cnt = Connection.Execute(sql, commandTimeout: CommandTimeout);
                OnAfterExecute?.Invoke(this, new SqlResultArgs(sql, cnt) { TableName = dest.TableFullName });
                return cnt;
            }
            else
            {
                var tmp = ReadTable(def.BridgeTableName);
                var orderSql = dest.SequenceColumn == null ? "" : $"order by {dest.SequenceColumn.ColumnName}";

                var sql = $@"
insert into {dest.TableFullName}(
{dest.Columns.Where(x => tmp.Columns.Contains(x)).ToString(",")}
)
select
{dest.Columns.Where(x => tmp.Columns.Contains(x)).ToString(",")}
from
{tmp.TableName}
{orderSql}
;";

                OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
                var cnt = Connection.Execute(sql, commandTimeout: CommandTimeout);
                OnAfterExecute?.Invoke(this, new SqlResultArgs(sql, cnt) { TableName = dest.TableFullName });
                return cnt;
            }
        }

        public int? InsertVersionTableOrDefault(SyncMap def)
        {
            if (def.VersionTable == null) return null;

            var args = DB.GetInsertVersionTableScalar(def);
            OnBeforeExecute?.Invoke(this, args);
            return Connection.ExecuteScalar<int>(args.Sql, args.Param, commandTimeout: CommandTimeout);
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
            var datasource = def.DatasourceTable;
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

            var sql = $@"
insert into {sync.TableFullName}
select distinct
    {columnsSql}
from
    {datasource.TableName}
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
            var datasource = def.DatasourceTable;

            var sql = $@"
insert into {map.TableFullName}
select
    {map.Columns.ToString(",")}
from
    {datasource.TableName}
order by
    {map.Columns.ToString(",")}
;
";
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
            return Connection.Execute(sql, commandTimeout: CommandTimeout);
        }
    }
}