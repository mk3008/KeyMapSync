using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity
{
    public class Destination
    {
        public int DestinationId { get; set; }

        public string DestinationName { get; set; }

        public string SequenceKeyColumn { get; set; }

        public string SequenceCommand { get; set; }

        public IList<string> Columns { get; set; }
    }
}
