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
        public BridgeLoad BuildBridge(Datasource datasource)
        {
            var dest = new DestinationMap()
            {
                TableName = Manager.GetNextBridgeName(),
                Columns = GetBridgeColumns(datasource)
            };

            var query = new BridgeLoad()
            {
                BridgeTableName = Manager.GetNextBridgeName(),
                AliasName = datasource.AliasName,
                WithQueryText = datasource.WithQueryText,
                Filter = datasource.ParameterSet ?? new ParameterSet(),
                Columns = GetBridgeColumns(datasource)
            };

            query.Filter = query.Filter.Merge(GetVersionFilter(datasource));
            query.KeyMap = KeyMapFilterBuilder.Build(datasource, query);

            return query;
        }

        private IEnumerable<string> GetBridgeColumns(IDatasource datasource)
        {
            var dest = Manager.ReadDestinationTable(datasource);
            var cols = new List<string>();

            cols.Add($"{datasource.AliasName}.*");
            cols.Add($"{dest.SequenceColumn.NextValCommand} as {dest.SequenceColumn.ColumnName}");

            var seq = Manager.GetVersionSeqColumnName(datasource);
            if (string.IsNullOrEmpty(seq)) cols.Add($":{seq} as {seq}");

            return cols;
        }

        private ParameterSet GetVersionFilter(IDatasource datasource)
        {
            var tbl = Manager.ReadOrCreateVersionTable(datasource);
            var col = tbl.SequenceColumn.ColumnName;
            var ver = Manager.GetNextVersion(tbl);
            var dic = new ExpandoObject() as IDictionary<string, object>;
            dic.Add(col, ver);
            return new ParameterSet() { Parameters = dic as ExpandoObject };
        }
    }
}
