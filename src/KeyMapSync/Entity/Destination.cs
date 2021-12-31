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

        public string SyncFormart { get; set; } = "{0}__sync";

        public string VersionFormat { get; set; } = "{0}__version";

        public string VersionKeyColumn { get; set; } = "version_id";

        public string NameColumn { get; set; } = "datasource_name";

        public string TimestampColumn { get; set; } = "create_timestamp";


        public string OffsetFormat { get; set; } = "{0}__offset";

        public string OffsetName => String.Format(OffsetFormat, DestinationName);


        public string OffsetColumnPrefix { get; set; } = "offset_";

        public string OffsetColumnName => $"{OffsetColumnPrefix}{SequenceKeyColumn}";

        public string RenewalColumnPrefix { get; set; } = "renewal_";

        public string RenewalColumnName => $"{RenewalColumnPrefix}{SequenceKeyColumn}";

        public string RemarksColumn { get; set; } = "remarks";

        /// <summary>
        /// ex
        /// "integration_sale_detail__sync"
        /// </summary>
        public string SyncName => string.Format(SyncFormart, DestinationName);

        public IList<string> GetSyncColumns()
        {
            var lst = new List<string>();
            lst.Add(SequenceKeyColumn);
            lst.Add(VersionKeyColumn);
            return lst;
        }

        /// <summary>
        /// ex
        /// "integration_sale_detail__version"
        /// </summary>
        public string VersionName => string.Format(VersionFormat, DestinationName);

        public IList<string> GetVersionColumns()
        {
            var lst = new List<string>();
            lst.Add(VersionKeyColumn);
            lst.Add(NameColumn);
            return lst;
        }

        public string VersionSequenceCommand { get; set; }
    }
}
