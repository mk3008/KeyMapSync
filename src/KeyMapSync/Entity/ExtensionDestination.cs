using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity
{
    public class ExtensionDestination
    {
        public int ExtensionDestinationId { get; set; }

        public Destination BaseDestination { get; set; }

        public string DestinationName { get; set; }

        public IList<string> Columns { get; set; }
    }
}
