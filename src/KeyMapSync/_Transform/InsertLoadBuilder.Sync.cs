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
        public DestinationLoad BuildSyncMap(Datasource datasource, BridgeLoad bridgemap)
        {
            var dest = Manager.ReadDestinationTable(datasource);
            var ver = Manager.ReadOrCreateVersionTable(datasource);
            var sync = Manager.ReadOrCreateSyncTable(datasource);


            var dsmap = new DatasourceAlias()
            {
                WithQueryText = datasource.WithQueryText,
                AliasName = datasource.AliasName,
                ParameterSet = datasource.ParameterSet,
            };

            var destmap = new DestinationMap()
            {
                TableName = dest.TableFullName,
                Columns = dest.Columns,
                AliasName = null,
            };

            var query = new DestinationLoad()
            {
                DatasourceMap = dsmap, 
                DestinationMap = destmap,
                LoadParameterSet

                DestinationTableName = sync.TableFullName,
                AliasName = datasource.AliasName,
                WithQueryText =
$@"with
{datasource.AliasName} as (select * from {bridgemap.BridgeTableName})",
                Filter = datasource.ParameterSet,
                Columns = new string[] {dest.SequenceColumn.ColumnName, ver.SequenceColumn.ColumnName}
            };
            return query;
        }
    }
}
