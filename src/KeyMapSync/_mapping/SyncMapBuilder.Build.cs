using System;
using System.Collections.Generic;
using System.Linq;

namespace KeyMapSync
{
    /// <summary>
    ///
    /// </summary>
    public partial class SyncMapBuilder
    {
        /// <summary>
        /// load <code>datasourceQuery</code> to <code>Destination</code>.
        /// </summary>
        public SyncMap Build(string destination, string mappingName, string datasourceQuery, string[] datasourceKeys, string datasourceAliasName = "datasource", Func<object> paramGenerator = null, bool isNeedExistsCheck = true)
        {
            var ds = new DatasourceMapWrap
            {
                DestinationTableName = destination,
                MappingName = mappingName,
                DatasourceAliasName = datasourceAliasName,
                DatasourceQuery = datasourceQuery,
                DatasourceKeyColumns = datasourceKeys,
                ParameterGenerator = paramGenerator,
                IsNeedExistsCheck = isNeedExistsCheck,
                IsExtension = false
            };

            return Build(ds, datasoruceName: "");
        }

        public SyncMap Build(ITableDatasourceMap tableDs)
        {
            var source = DbExecutor.ReadTable(tableDs.DatasourceTableName);
            var ds = new DatasourceMapWrap
            {
                DestinationTableName = tableDs.DestinationTableName,
                MappingName = tableDs.MappingName,
                DatasourceQuery = tableDs.DatasourceQuery,
                DatasourceAliasName = tableDs.DatasourceAliasName,
                DatasourceKeyColumns = new string[] { source.SequenceColumn.ColumnName },
                ParameterGenerator = tableDs.ParameterGenerator,
                IsNeedExistsCheck = true,
                IsExtension = false,
            };

            foreach (var item in tableDs?.Cascades)
            {
                ds.Cascades.Add(item);
            }

            return Build(ds, datasoruceName: tableDs.GetType().FullName);
        }

        public SyncMap Build(IDatasourceMap ds, SyncMap sender = null, string datasoruceName = null)
        {
            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");

            if (string.IsNullOrEmpty(SyncTableSuffix)) throw new InvalidOperationException("'SyncTableSuffix' property is null.");
            if (string.IsNullOrEmpty(SequenceColumnSuffix)) throw new InvalidOperationException("'SequenceTableSuffix' property is null.");

            if (ds == null) throw new InvalidOperationException("'IDatasourceMap' is null.");

            if (sender == null && ds.IsExtension) throw new InvalidOperationException("extension datasource must have sender.");

            // destination table exists check
            var dest = DbExecutor.ReadTable(ds.DestinationTableName);

            //if (dest == null) throw new InvalidOperationException($"Destination table ({ds.DestinationTableName}) is not exists.");

            // datasoruceName
            if (datasoruceName == null) datasoruceName = ds.GetType().FullName;

            if (ds.IsExtension)
            {
                var datasource = new DatasourceTable
                {
                    TableName = sender.DatasourceTable.TableName,
                    IsMustCreate = false
                };

                var def = new SyncMap
                {
                    MappingName = null,
                    DestinationTable = dest,
                    VersionTable = null,
                    SyncTable = null,
                    MappingTable = null,
                    DatasourceTable = datasource,
                    IsNeedExistsCheck = ds.IsNeedExistsCheck,
                    DatasourceMap = ds,
                    Sender = sender,
                    DatasourceName = datasoruceName
                };

                return def;
            }
            else
            {
                // automatic generation of versionTable
                Table version = ReadOrCreateVersionTableOrDefault(dest);

                // automatic generation of syncTable
                Table sync = ReadOrCreateSyncTableOrDefault(version, dest);

                // automatic generation of mappingTable
                Table map = ReadOrCreateMappingTableOrDefault(ds.MappingName, dest, ds.DatasourceKeyColumns); ;

                // ceate temporary table, and insert 'DestinationTable', 'SyncTable', 'MappingTable'.
                var db = DbExecutor.DB;
                var sufix = $"_{DateTime.Now.ToString("ssffff")}";
                var tblName = (map == null) ? "keymapsync" : $"{map.TableName.Left(db.TableNameMaxLength - sufix.Length)}";
                var datasource = new DatasourceTable
                {
                    TableName = $"{tblName}{sufix}",
                    IsMustCreate = true
                };

                var def = new SyncMap
                {
                    MappingName = ds.MappingName,
                    DestinationTable = dest,
                    VersionTable = version,
                    SyncTable = sync,
                    MappingTable = map,
                    DatasourceTable = datasource,
                    IsNeedExistsCheck = ds.IsNeedExistsCheck,
                    DatasourceMap = ds,
                    Sender = sender,
                    DatasourceName = datasoruceName
                };

                return def;
            }
        }
    }
}