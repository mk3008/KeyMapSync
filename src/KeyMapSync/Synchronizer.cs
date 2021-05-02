using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public partial class Synchronizer
    {
        public Synchronizer(DbExecutor executor)
        {
            DbExecutor = executor;
        }

        public Synchronizer(SyncMapBuilder builder)
        {
            DbExecutor = builder.DbExecutor;
            Builder = builder;
        }

        /// <summary>
        /// database command executor
        /// </summary>
        private DbExecutor DbExecutor { get; set; }

        private SyncMapBuilder Builder { get; set; }

        public Result Result { get; private set; }
    }
}