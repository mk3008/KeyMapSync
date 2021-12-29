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
        /// <summary>
        /// create temporary table.
        /// </summary>
        /// <param name="def"></param>
        public int CreateBridgeOrDefault(SyncMap def)
        {
            if (def.DatasourceMap.IsExtension)
            {
                return GetDatasourceRowCount(def);
            }
            else if(def.KeyMap == null || def.DatasourceMap.IsNeedExistsCheck == false )
            {
                return CreateBridgeNoCheck(def);
            }
            else
            {
                return CreateBridge(def);
            }
        }

        private int GetDatasourceRowCount(SyncMap def)
        {
            var s = $"{def.DatasourceMap.DatasourceQueryGenarator(def.Sender)} select count(*) from {def.DatasourceMap.DatasourceAliasName}";
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(s));
            return Connection.ExecuteScalar<int>(s, commandTimeout: CommandTimeout);
        }

        private int CreateBridge(SyncMap def)
        {
            var ds = def.DatasourceMap;
            var map = def.KeyMap.MappingTable;
            var dst = def.DestinationTable;

            var seq = (dst?.SequenceColumn == null || def.DatasourceMap.IsBridge) ? "" : $"{dst.SequenceColumn.NextValCommand} as { dst.SequenceColumn.ColumnName}, ";

            var orderText = (ds?.DatasourceKeyColumns == null) ? "" : $"order by {ds.DatasourceKeyColumns.ToString(", ", x => $"{ds.DatasourceAliasName}.{x}")}";

            var where = !def.DatasourceMap.IsNeedExistsCheck ? "" : $"where not exists (select * from {map.TableFullName} x where {ds.DatasourceKeyColumns.ToString(" and ", x => $"x.{x} = {ds.DatasourceAliasName}.{x}")})";

            var sql = $@"
create temporary table {def.BridgeTableName}
as
{ds.DatasourceQueryGenarator(def.Sender)}
select
    {seq}{ds.DatasourceAliasName}.*
from
    {ds.DatasourceAliasName}
{where}
{orderText}
";
            ExpandoObject prm = ds.ParameterGenerator?.Invoke();
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
            Connection.Execute(sql, prm, commandTimeout: CommandTimeout);

            //retrun insert count
            var cntSql = $"select count(*) from {def.BridgeTableName}";
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(cntSql));

            var cnt = Connection.ExecuteScalar<int>(cntSql, commandTimeout: CommandTimeout);
            OnAfterExecute?.Invoke(this, new SqlResultArgs(sql, cnt, prm) { TableName = def.BridgeTableName });

            return cnt;
        }

        private int CreateBridgeNoCheck(SyncMap def)
        {
            var ds = def.DatasourceMap;
            var dst = def.DestinationTable;

            var seq = (dst?.SequenceColumn == null || def.DatasourceMap.IsBridge) ? "" : $"{dst.SequenceColumn.NextValCommand} as { dst.SequenceColumn.ColumnName}, ";

            var orderText = (ds?.DatasourceKeyColumns == null) ? "" : $"order by {ds.DatasourceKeyColumns.ToString(", ", x => $"{ds.DatasourceAliasName}.{x}")}";

            var sql = $@"
create temporary table {def.BridgeTableName}
as
{ds.DatasourceQueryGenarator(def.Sender)}
select
    {seq}{ds.DatasourceAliasName}.*
from
    {ds.DatasourceAliasName}
{orderText}
";
            ExpandoObject prm = ds.ParameterGenerator?.Invoke();
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
                return InsertExtension(def);
            }
            else
            {
                return InsertDestination(def);
            }
        }

        private int InsertExtension(SyncMap def)
        {
            var dest = def.DestinationTable;
            var bridge = ReadTable(def.BridgeTableName);

            var orderSql = dest.SequenceColumn == null ? "" : $"order by {dest.SequenceColumn.ColumnName}";

            var sql = $@"
insert into {dest.TableFullName}(
{dest.Columns.Where(x => bridge.Columns.Contains(x)).ToString(",")}
)
{def.DatasourceMap.DatasourceQueryGenarator(def.Sender)}
select
{dest.Columns.Where(x => bridge.Columns.Contains(x)).ToString(",")}
from
{def.DatasourceMap.DatasourceAliasName}
{orderSql}
;";

            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
            var cnt = Connection.Execute(sql, commandTimeout: CommandTimeout);
            OnAfterExecute?.Invoke(this, new SqlResultArgs(sql, cnt) { TableName = dest.TableFullName });
            return cnt;
        }

        private int InsertDestination(SyncMap def)
        {
            var dest = def.DestinationTable;
            var bridge = ReadTable(def.BridgeTableName);

            var orderSql = dest.SequenceColumn == null ? "" : $"order by {dest.SequenceColumn.ColumnName}";

            var sql = $@"
insert into {dest.TableFullName}(
{dest.Columns.Where(x => bridge.Columns.Contains(x)).ToString(",")}
)
select
{dest.Columns.Where(x => bridge.Columns.Contains(x)).ToString(",")}
from
{bridge.TableName}
{orderSql}
;";

            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
            var cnt = Connection.Execute(sql, commandTimeout: CommandTimeout);
            OnAfterExecute?.Invoke(this, new SqlResultArgs(sql, cnt) { TableName = dest.TableFullName });
            return cnt;
        }

        public int? InsertVersionTableOrDefault(SyncMap def)
        {
            if (def.KeyMap.VersionTable == null) return null;

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
            var sync = def.KeyMap.SyncTable;
            var version = def.KeyMap.VersionTable;
            var bridge = ReadTable(def.BridgeTableName);
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
    {bridge.TableName}
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
            var map = def.KeyMap.MappingTable;
            var bridge = ReadTable(def.BridgeTableName);

            var sql = $@"
insert into {map.TableFullName}
select
    {map.Columns.ToString(",")}
from
    {bridge.TableName}
order by
    {map.Columns.ToString(",")}
;
";
            OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql));
            return Connection.Execute(sql, commandTimeout: CommandTimeout);
        }
    }
}