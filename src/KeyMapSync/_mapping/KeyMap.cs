using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public class KeyMap
    {
        public string MappingName { get; set; }

        public Table VersionTable { get; set; }

        public Table SyncTable { get; set; }

        public Table MappingTable { get; set; }
    }
}