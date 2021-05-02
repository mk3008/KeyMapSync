using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KeyMapSync
{
    /// <summary>
    /// string extension
    /// </summary>
    internal static class SyncMapExtensions
    {
        public static SyncMap ToOffsetSyncMap(this SyncMap source, SyncMapBuilder builder, IEnumerable<string> valuesColumns, int id)
        {
            var dest = source.DestinationTable;
            var keyName = $"origin_{dest.SequenceColumn.ColumnName}";
            var mappingName = $"origin_{dest.TableName}";

            // generate offsetdatasource from datasource
            var sql = @$"
with datasource as (
select
    {dest.SequenceColumn.ColumnName} as {keyName}
    , {dest.Columns.Where(x => x != dest.SequenceColumn.ColumnName).Select(x => valuesColumns.Contains(x) ? $"{x} * -1 as {x}" : x).ToString(",")}
from
    {dest.TableFullName}
where
    {dest.SequenceColumn.ColumnName} = :id
)
";
            var ds = new DatasourceMap { DestinationTableName = dest.TableName, MappingName = dest.TableName, DatasourceQuery = sql, DatasourceAliasName = "datasource", DatasourceKeyColumns = new string[] { keyName }, ParameterGenerator = () => new { id }, IsNeedExistsCheck = true };
            return builder.Build(ds);
        }

        public static SyncMap ToOffsetVersionSyncMap(this SyncMap source, SyncMapBuilder builder, IEnumerable<string> valuesColumns, int version)
        {
            var dest = source.DestinationTable;
            var keyName = $"origin_{dest.SequenceColumn.ColumnName}";
            var mappingName = $"origin_{dest.TableName}";
            var sync = source.SyncTable;
            var ver = source.VersionTable;

            // generate offsetdatasource from datasource
            var sql = @$"
with datasource as (
select
    {dest.SequenceColumn.ColumnName} as {keyName}
    , {dest.Columns.Where(x => x != dest.SequenceColumn.ColumnName).Select(x => valuesColumns.Contains(x) ? $"{x} * -1 as {x}" : x).ToString(",")}
from
    {dest.TableFullName}
where
    exists (select * from {sync.TableFullName} x where x.{ver.SequenceColumn.ColumnName} = :version
)
";
            var ds = new DatasourceMap { DestinationTableName = dest.TableName, MappingName = dest.TableName, DatasourceQuery = sql, DatasourceAliasName = "datasource", DatasourceKeyColumns = new string[] { keyName }, ParameterGenerator = () => new { version }, IsNeedExistsCheck = true };
            return builder.Build(ds);
        }
    }
}