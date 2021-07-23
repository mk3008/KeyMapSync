using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    /// <summary>
    /// Manages mapping information used for <code>Synchronizer</code>
    /// </summary>
    public class SyncMap
    {
        public SyncMap Sender { get; set; }

        public string MappingName { get; set; }

        public Table DestinationTable { get; set; }

        public Table VersionTable { get; set; }

        public Table SyncTable { get; set; }

        public Table MappingTable { get; set; }

        [Obsolete("use BridgeTableName")]
        public DatasourceTable DatasourceTable => new DatasourceTable() { TableName = BridgeTableName };

        public string BridgeTableName { get; set; }

        public IDatasourceMap DatasourceMap { get; set; }

        public string DatasourceName { get; set; }

        public string BridgeChainName => GetBridgeChainName();

        public string GetBridgeChainName()
        {
            var s = BridgeTableName;

            if (DestinationTable != null)
            {
                s = $"{s}->{DestinationTable.TableName}";
            }

            if (Sender == null) return s;

            return $"{Sender.BridgeChainName}->{s}";
        }
    }
}