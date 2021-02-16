using System;
using System.Collections.Generic;
using System.Linq;

namespace KeyMapSync
{
    /// <summary>
    ///
    /// </summary>
    public class MappingDefinitionBuilder
    {
        /// <summary>
        /// database command executor
        /// </summary>
        public DbExecutor DbExecutor { get; set; }

        /// <summary>
        /// destination table name
        /// </summary>
        public string Destination { get; set; }

        /// <summary>
        /// sync version table naming conventions
        /// </summary>
        public string SyncVersionTableSuffix { get; set; } = "sync_version";

        /// <summary>
        /// sync table naming conventions
        /// </summary>
        public string SyncTableSuffix { get; set; } = "sync";

        /// <summary>
        /// keymap table naming conventions
        /// </summary>
        public string KeyMapTablePrefix { get; set; } = "map";

        /// <summary>
        /// sequence column naming conventions
        /// </summary>
        public string SequenceColumnSuffix { get; set; } = "id";

        /// <summary>
        /// load <code>datasourceQuery</code> to <code>Destination</code>.
        /// </summary>
        /// <param name="mappingName">any name.used for management table name</param>
        /// <param name="datasourceQuery">With clause that defines an alias called 'datasource'. ex.<code>with datasource as (select * from client)</code></param>
        public MappingDefinition Build(string mappingName, string datasourceQuery)
        {
            return Build(mappingName, datasourceQuery, mappingName);
        }

        /// <summary>
        /// load <code>datasourceQuery</code> to <code>Destination</code>.
        /// </summary>
        /// <param name="mappingName">any name.used for management table name</param>
        /// <param name="datasourceQuery">With clause that defines an alias called 'datasource'. ex.<code>with datasource as (select * from client)</code></param>
        /// <param name="datasourceName">datasource table name. get sequence information from table name.</param>
        public MappingDefinition Build(string mappingName, string datasourceQuery, string datasourceName, string datasourceAliasName = "datasource", Func<object> paramGenerator = null)
        {
            var source = DbExecutor.ReadTable(datasourceName);
            var ds = new Datasource { MappingName = mappingName, DatasourceQuery = datasourceQuery, DatasourceAliasName = datasourceAliasName, DatasourceKeyColumns = new string[] { source.SequenceColumn.ColumnName }, ParameterGenerator = paramGenerator };
            return Build(ds);
        }

        public MappingDefinition Build(IDatasource ds)
        {
            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");

            if (string.IsNullOrEmpty(Destination)) throw new InvalidOperationException("'Destination' property is null.");
            if (string.IsNullOrEmpty(SyncTableSuffix)) throw new InvalidOperationException("'SyncTableSuffix' property is null.");
            if (string.IsNullOrEmpty(SequenceColumnSuffix)) throw new InvalidOperationException("'SequenceTableSuffix' property is null.");

            if (ds == null) throw new InvalidOperationException("'datasource' is null.");
            if (string.IsNullOrEmpty(ds.DatasourceQuery)) throw new InvalidOperationException("'DatasourceQuery' property is null.");

            if (ds.DatasourceKeyColumns.Any() == false)
            {
                var singleDs = ds as SingleTableDatasource;
                ds = ConvertToDatasource(singleDs);
            }

            // destination table exists check
            var dest = DbExecutor.ReadTable(Destination);
            if (dest == null) throw new InvalidOperationException($"Destination table ({Destination}) is not exists.");

            // automatic generation of versionTable
            var version = ReadOrCreateVersionTable();

            // automatic generation of syncTable
            var sync = ReadOrCreateSyncTable(version, dest);

            // automatic generation of mappingTable
            var map = ReadOrCreateMappingTable(ds.MappingName, dest, ds.DatasourceKeyColumns);

            // ceate temporary table, and insert 'DestinationTable', 'SyncTable', 'MappingTable'.
            var tmp = new TemporaryTable()
            {
                TableName = $"{map.TableName}_{DateTime.Now.ToString("mmffffff")}",
                DatasourceQuery = ds.DatasourceQuery,
                DatasourceAliasName = ds.DatasourceAliasName,
                DestinationSequence = dest.SequenceColumn,
                SourceKeyColumns = ds.DatasourceKeyColumns,
                ParamGenerator = ds.ParameterGenerator
            };

            var def = new MappingDefinition
            {
                MappingName = ds.MappingName,
                Destination = dest,
                VersionTable = version,
                SyncTable = sync,
                MappingTable = map,
                TemporaryTable = tmp,
            };

            return def;
        }

        private Table ReadOrCreateVersionTable()
        {
            var name = $"{Destination}_{SyncVersionTableSuffix}";
            var table = DbExecutor.ReadTable(name);
            if (table == null)
            {
                DbExecutor.CreateSyncVersionTable(name, $"{name}_{SequenceColumnSuffix}");
                table = DbExecutor.ReadTable(name);
            }
            return table;
        }

        private Table ReadOrCreateSyncTable(Table version, Table dest)
        {
            var name = $"{Destination}_{SyncTableSuffix}";
            var table = DbExecutor.ReadTable(name);
            if (table == null)
            {
                DbExecutor.CreateSyncTable(name, dest, version);
                table = DbExecutor.ReadTable(name);
            }
            return table;
        }

        private Table ReadOrCreateMappingTable(string mappingName, Table dest, IEnumerable<string> uniqueKeyColumns)
        {
            var name = $"{Destination}_{KeyMapTablePrefix}_{mappingName}";
            var table = DbExecutor.ReadMappingTableInfo(name);
            if (table == null)
            {
                DbExecutor.CreateMappingTable(name, dest, uniqueKeyColumns);
                table = DbExecutor.ReadMappingTableInfo(name);
            }
            return table;
        }

        public IDatasource ConvertToDatasource(SingleTableDatasource singleDs)
        {
            var source = DbExecutor.ReadTable(singleDs.DatasourceTableName);
            var ds = new Datasource { MappingName = singleDs.MappingName, DatasourceQuery = singleDs.DatasourceQuery, DatasourceAliasName = singleDs.DatasourceAliasName, DatasourceKeyColumns = new string[] { source.SequenceColumn.ColumnName }, ParameterGenerator = singleDs.ParameterGenerator };
            return ds;
        }
    }
}