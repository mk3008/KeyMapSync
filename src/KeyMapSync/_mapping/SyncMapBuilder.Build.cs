﻿using System;
using System.Collections.Generic;
using System.Dynamic;
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
        public SyncMap Build(string destination, string mappingName, string datasourceQuery, string[] datasourceKeys, string datasourceAliasName = "datasource", Func<ExpandoObject> paramGenerator = null, bool isNeedExistsCheck = true)
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

        public SyncMap Build(IDatasourceMappable ds)
        {
            if (ds is ITableDatasourceMap) return Build(ds as ITableDatasourceMap);
            if (ds is IDatasourceMap) return Build(ds as IDatasourceMap);

            throw new NotSupportedException();
        }

        private SyncMap Build(ITableDatasourceMap tableDs)
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

        internal SyncMap Build(IDatasourceMap ds, SyncMap sender = null, string datasoruceName = null, string prefix = null)
        {
            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");

            if (string.IsNullOrEmpty(SyncTableSuffix)) throw new InvalidOperationException("'SyncTableSuffix' property is null.");
            if (string.IsNullOrEmpty(SequenceColumnSuffix)) throw new InvalidOperationException("'SequenceTableSuffix' property is null.");

            if (ds == null) throw new InvalidOperationException("'IDatasourceMap' is null.");

            if (sender == null && ds.IsExtension) throw new InvalidOperationException("extension datasource must have sender.");

            // destination table exists check
            var dest = DbExecutor.ReadTable(ds.DestinationTableName);

            if (dest == null)
            {
                throw new InvalidOperationException($"Destination table ({ds.DestinationTableName}) is not exists.");
            }

            // datasoruceName
            if (datasoruceName == null) datasoruceName = ds.GetType().FullName;

            SyncMap def = null;
            if (ds.IsExtension)
            {
                def = BuildExtension(ds, sender, datasoruceName, dest);
            }
            else if (ds.MappingName == null)
            {
                def = BuildNoTrace(ds, sender, datasoruceName, dest, prefix);
            }
            else
            {
                def = Build(ds, sender, datasoruceName, dest, prefix);
            }

            //cascade
            foreach (var item in ds.Cascades)
            {
                def.Cascades.Add(Build(item, def));
            }

            return def;
        }

        private SyncMap BuildExtension(IDatasourceMap ds, SyncMap sender, string datasoruceName, Table dest)
        {
            var def = new SyncMap
            {
                DestinationTable = dest,
                BridgeTableName = sender.BridgeTableName,
                DatasourceMap = ds,
                Sender = sender,
                DatasourceName = datasoruceName
            };
            return def;
        }

        private SyncMap Build(IDatasourceMap ds, SyncMap sender, string datasoruceName, Table dest, string prefix)
        {
            // automatic generation of versionTable
            Table version = ReadOrCreateVersionTableOrDefault(dest);

            // automatic generation of syncTable
            Table sync = ReadOrCreateSyncTableOrDefault(version, dest);

            // automatic generation of mappingTable
            Table map = ReadOrCreateMappingTableOrDefault(ds.MappingName, dest, ds.DatasourceKeyColumns); ;

            // ceate temporary table, and insert 'DestinationTable', 'SyncTable', 'MappingTable'.
            var db = DbExecutor.DB;
            var sufix = $"_tmp_{DateTime.Now.ToString("ssffff")}";
            //var tblName = (map == null) ? "keymapsync" : $"{map.TableName.Left(db.TableNameMaxLength - sufix.Length)}";
            var tblName = $"{prefix}{dest.TableName}";
            tblName = tblName.Left(db.TableNameMaxLength - sufix.Length);

            var keymap = new KeyMap
            {
                MappingName = ds.MappingName,
                VersionTable = version,
                SyncTable = sync,
                MappingTable = map,
            };

            var def = new SyncMap
            {
                KeyMap = keymap,
                DestinationTable = dest,
                BridgeTableName = $"{tblName}{sufix}",
                DatasourceMap = ds,
                Sender = sender,
                DatasourceName = datasoruceName
            };

            return def;
        }

        private SyncMap BuildNoTrace(IDatasourceMap ds, SyncMap sender, string datasoruceName, Table dest, string prefix)
        {
            // ceate temporary table, and insert 'DestinationTable', 'SyncTable', 'MappingTable'.
            var db = DbExecutor.DB;
            var sufix = $"_tmp_{DateTime.Now.ToString("ssffff")}";
            //var tblName = (map == null) ? "keymapsync" : $"{map.TableName.Left(db.TableNameMaxLength - sufix.Length)}";
            var tblName = $"{prefix}{dest.TableName}";
            tblName = tblName.Left(db.TableNameMaxLength - sufix.Length);

            var def = new SyncMap
            {
                DestinationTable = dest,
                BridgeTableName = $"{tblName}{sufix}",
                DatasourceMap = ds,
                Sender = sender,
                DatasourceName = datasoruceName
            };

            return def;
        }
    }
}