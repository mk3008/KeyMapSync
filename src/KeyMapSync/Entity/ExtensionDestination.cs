using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity
{
    public class ExtensionDestination
    {
        public string DestinationName { get; set; }= string.Empty;

        public List<string> Columns { get; set; } = new();
    }
}
