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

        public string OffsetFormat { get; set; } = "{0}__offset";

        public string OffsetName => String.Format(OffsetFormat, DestinationName);


        public string OffsetColumnPrefix { get; set; } = "offset_";

        public string OffsetColumnName => $"{OffsetColumnPrefix}{SequenceKeyColumn}";

        public string RenewalColumnPrefix { get; set; } = "renewal_";

        public string RenewalColumnName => $"{RenewalColumnPrefix}{SequenceKeyColumn}";

        public string RemarksColumn { get; set; } = "remarks";

    }
}
