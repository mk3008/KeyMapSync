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
        private DestinationLoad BuildExtensionMapping(ExtensionDatasource datasource, BridgeLoad bridgemap)
        {
            var query = new DestinationLoad()
            {
                DestinationTableName = datasource.DestinaionTableName,
                AliasName = datasource.AliasName,
                WithQueryText =
$@"with
{datasource.AliasName} as (select * from {bridgemap.BridgeTableName})",
                Filter = datasource.ParameterSet,
                Columns = Manager.ReadDestinationTable(datasource).Columns
            };

            return query;
        }
    }
}
