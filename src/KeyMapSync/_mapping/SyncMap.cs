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
        public string MappingName { get; set; }

        public Table DestinationTable { get; set; }

        public Table VersionTable { get; set; }

        public Table SyncTable { get; set; }

        public Table MappingTable { get; set; }

        public TemporaryTable TemporaryTable { get; set; }

        public bool IsNeedExistsCheck { get; set; } = true;
    }
}