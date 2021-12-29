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
        private Table ReadOrCreateVersionTableOrDefault(Table dest)
        {
            if (dest?.SequenceColumn == null) return null;

            var name = $"{dest.TableName}_{SyncVersionTableSuffix}";
            var table = DbExecutor.ReadTable(name);
            if (table == null)
            {
                DbExecutor.CreateVersionTable(name, $"{name}_{SequenceColumnSuffix}");
                table = DbExecutor.ReadTable(name);
            }
            return table;
        }

        private Table ReadOrCreateSyncTableOrDefault(Table version, Table dest)
        {
            if (version == null) return null;

            var name = $"{dest.TableName}_{SyncTableSuffix}";
            var table = DbExecutor.ReadTable(name);
            if (table == null)
            {
                DbExecutor.CreateSyncTable(name, dest, version);
                table = DbExecutor.ReadTable(name);
            }

            //sync-table has not sequence column.
            table.SequenceColumn = null;

            return table;
        }

        private Table ReadOrCreateMappingTableOrDefault(string mappingName, Table dest, IEnumerable<string> uniqueKeyColumns)
        {
            if (mappingName == null) return null;
            if (dest?.SequenceColumn == null) return null;
            if (!uniqueKeyColumns.Any()) return null;

            var name = $"{dest.TableName}_{KeyMapTablePrefix}_{mappingName}";
            var table = DbExecutor.ReadMappingTableInfo(name);
            if (table == null)
            {
                DbExecutor.CreateMappingTable(name, dest, uniqueKeyColumns);
                table = DbExecutor.ReadMappingTableInfo(name);
            }

            return table;
        }
    }
}