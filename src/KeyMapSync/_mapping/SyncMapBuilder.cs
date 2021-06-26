using System;
using System.Collections.Generic;
using System.Linq;

namespace KeyMapSync
{
    /// <summary>
    ///
    /// </summary>
    public partial class SyncMapBuilder
    {
        /// <summary>
        /// database command executor
        /// </summary>
        public DbExecutor DbExecutor { get; set; }

        /// <summary>
        /// sync version table naming conventions
        /// </summary>
        public string SyncVersionTableSuffix { get; set; } = "sync_version";

        /// <summary>
        /// sync table naming conventions
        /// </summary>
        public string SyncTableSuffix { get; set; } = "sync";

        /// <summary>
        /// keymap table naming conventions
        /// </summary>
        public string KeyMapTablePrefix { get; set; } = "map";

        /// <summary>
        /// sequence column naming conventions
        /// </summary>
        public string SequenceColumnSuffix { get; set; } = "id";
    }
}