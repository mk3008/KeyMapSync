using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity
{
    public class Destination
    {
        /// <summary>
        /// Destination table name.
        /// </summary>
        public string DestinationName { get; set; } = String.Empty;

        public List<GroupDestination> Groups { get; set; } = new();

        /// <summary>
        /// Sequence key column name.
        /// </summary>
        public string SequenceKeyColumn { get; set; } = String.Empty;

        /// <summary>
        /// Sequence next value command.
        /// ex.
        /// "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over()"
        /// </summary>
        public string SequenceCommand { get; set; } = String.Empty;

        /// <summary>
        /// Destination columns.
        /// </summary>
        public IList<string> Columns { get; init; } = new List<string>();

        /// <summary>
        /// ex
        /// "quantity, price"
        /// </summary>
        public IList<string> SingInversionColumns { get; init; } = new List<string>();

        /// <summary>
        /// Sync-table name format.
        /// </summary>
        public string SyncFormart { get; set; } = "{0}__sync";

        /// <summary>
        /// Version-table name format.
        /// </summary>
        public string VersionFormat { get; set; } = "{0}__version";

        /// <summary>
        /// Version key column name.
        /// </summary>
        public string VersionKeyColumn { get; set; } = "version_id";

        /// <summary>
        /// Datasourcename column name.
        /// </summary>
        public string NameColumn { get; set; } = "datasource_name";

        /// <summary>
        /// Timestamp column name.
        /// </summary>
        public string TimestampColumn { get; set; } = "create_timestamp";

        /// <summary>
        /// Offset-table name format.
        /// </summary>
        public string OffsetFormat { get; set; } = "{0}__offset";

        /// <summary>
        /// Offest-table name.
        /// </summary>
        public string OffsetName => String.Format(OffsetFormat, DestinationName);

        /// <summary>
        /// Offset column prefix.
        /// </summary>
        public string OffsetColumnPrefix { get; set; } = "offset_";

        /// <summary>
        /// Offset column name.
        /// </summary>
        public string OffsetColumnName => $"{OffsetColumnPrefix}{SequenceKeyColumn}";

        /// <summary>
        /// Renewal column prefix.
        /// </summary>
        public string RenewalColumnPrefix { get; set; } = "renewal_";

        /// <summary>
        /// Renewal column name.
        /// </summary>
        public string RenewalColumnName => $"{RenewalColumnPrefix}{SequenceKeyColumn}";

        /// <summary>
        /// Offest remarks column
        /// </summary>
        public string OffsetRemarksColumn { get; set; } = "remarks";

        /// <summary>
        /// ex
        /// "integration_sale_detail__sync"
        /// </summary>
        public string SyncName => string.Format(SyncFormart, DestinationName);

        /// <summary>
        /// ex
        /// "integration_sale_detail__version"
        /// </summary>
        public string VersionName => string.Format(VersionFormat, DestinationName);

        /// <summary>
        /// Get version-table column list.
        /// </summary>
        /// <returns></returns>
        public IList<string> GetVersionColumns()
        {
            var lst = new List<string>();
            lst.Add(VersionKeyColumn);
            lst.Add(NameColumn);
            return lst;
        }

        /// <summary>
        /// Version sequence next value command.
        /// </summary>
        public string VersionSequenceCommand { get; set; } = String.Empty;
    }
}
