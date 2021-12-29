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
        private DestinationMap BuildDestinationMap(Datasource datasource)
        {
            var tbl = Manager.ReadTable(datasource.DestinaionTableName);
            var map = new DestinationMap()
            {
                TableName = tbl.TableFullName,
                GetColumns = tbl.Columns,
                AliasName = null,
            };
            return map;
        }

        private DestinationLoad BuildDestination(Datasource datasource, BridgeLoad bridge)
        {
            var scanner = new Func<IEnumerable<string>>(() => Manager.GetColumns(bridge.DestinationMap.TableName));

            var dsmap = new DatasourceAlias
            {
                //with _ds as (select * from bridge)
                WithQueryText = $"with {datasource.AliasName} as (select * from {bridge.DestinationMap.TableName})",
                AliasName = datasource.AliasName,
            };

            var query = new DestinationLoad()
            {
                ColumnScanner = scanner,
                DestinationMap = BuildDestinationMap(datasource),
                DatasourceMap = dsmap,
            };

            query.LoadQueryables.Add(BuildSyncMap(datasource, bridge));
            query.LoadQueryables.Add(BuildVersionMap(datasource, bridge));
            query.LoadQueryables.Add(BuildKeyMap(datasource, bridge));

            datasource.Extensions.Action(x => query.LoadQueryables.Add(Build(x, bridge)));

            return query;
        }

        private DestinationLoad BuildDestinationMapping(Datasource datasource)
        {
            var dsmap = new DatasourceAlias
            {
                //with _ds as (select * from datasource)
                WithQueryText = datasource.WithQueryText,
                AliasName = datasource.AliasName,
                ParameterSet = datasource.ParameterSet,
            };

            var query = new DestinationLoad()
            {
                ColumnScanner = null,
                DestinationMap = BuildDestinationMap(datasource),
                DatasourceMap = dsmap,
            };

            datasource.Extensions.Action(x => query.LoadQueryables.Add(Build(x)));

            return query;
        }

        private IEnumerable<string> GetDestinationColumns(Datasource datasource)
        {
            var dest = Manager.ReadDestinationTable(datasource);
            var cols = new List<string>();

            foreach (var item in dest.Columns)
            {
                if (dest.SequenceColumn.ColumnName == item)
                {
                    cols.Add($"{dest.SequenceColumn.NextValCommand} as {dest.SequenceColumn.ColumnName}");
                }
                else
                {
                    cols.Add(item);
                }
            }

            var seq = Manager.GetVersionSeqColumnName(datasource);
            if (string.IsNullOrEmpty(seq)) cols.Add($":{seq} as {seq}");

            return cols;
        }

        private IEnumerable<string> GetDestinationColumns(IDatasource datasource, BridgeLoad bridgemap)
        {
            var bridgecols = Manager.ReadTable(bridgemap.BridgeTableName).Columns; //no cache
            var destcols = Manager.ReadDestinationTable(datasource).Columns;

            return destcols.Where(x => destcols.Contains(x));
        }
    }
}
