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
        public DestinationLoad BuildVersionMap(Datasource datasource, BridgeLoad bridge)
        {
            var ver = Manager.ReadOrCreateVersionTable(datasource);
            var sync = Manager.ReadOrCreateSyncTable(datasource);

            var query = new DestinationLoad()
            {
                DestinationTableName = sync.TableFullName,
                AliasName = datasource.AliasName,
                WithQueryText =
$@"with
{datasource.AliasName} as (select distinct {ver.SequenceColumn.ColumnName}, :datasource_name as datasource_name, :mapping_name as mapping_name from {bridge.BridgeTableName})",
                Filter = datasource.ParameterSet ?? new ParameterSet(),
                Columns = new string[] { ver.SequenceColumn.ColumnName }
            };

            var dic = new ExpandoObject() as IDictionary<string, object>;
            dic.Add(":datasource_name", datasource.DatasourceName);
            dic.Add(":mapping_name", datasource.MappingName);
            var filter = new ParameterSet() { Parameters = dic as ExpandoObject };

            query.Filter = query.Filter.Merge(GetVersionInfoFilter(datasource));
            return query;
        }

        private ParameterSet GetVersionInfoFilter(Datasource datasource)
        {
            var dic = new ExpandoObject() as IDictionary<string, object>;
            dic.Add(":datasource_name", datasource.DatasourceName);
            dic.Add(":mapping_name", datasource.MappingName);
            return new ParameterSet() { Parameters = dic as ExpandoObject };
        }
    }
}
