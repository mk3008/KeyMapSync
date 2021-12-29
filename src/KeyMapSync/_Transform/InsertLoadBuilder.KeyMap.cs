using Dapper;
using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform
{
    public partial class InsertLoadBuilder
    {
        public DestinationLoad BuildKeyMap(Datasource datasource, BridgeLoad bridgemap)
        {
            var dest = Manager.ReadDestinationTable(datasource);
            var keymap = Manager.ReadOrCreateKeyMapTable(datasource);

            var cols = datasource.DatasourceKeyColumns.ToList();
            cols.Add(dest.SequenceColumn.ColumnName);

            var query = new DestinationLoad()
            {
                DestinationTableName = keymap.TableFullName,
                AliasName = datasource.AliasName,
                WithQueryText =
$@"with
{datasource.AliasName} as (select * from {bridgemap.BridgeTableName})",
                Filter = datasource.ParameterSet,
                Columns = cols
            };
            return query;
        }
    }
}
