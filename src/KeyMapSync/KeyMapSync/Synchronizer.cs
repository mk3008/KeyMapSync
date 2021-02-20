using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public class Synchronizer
    {
        /// <summary>
        /// database command executor
        /// </summary>
        public DbExecutor DbExecutor { private get; set; }

        public Result Result { get; private set; }

        public void Insert(SyncMap def)
        {
            Result = null;

            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");

            var count = DbExecutor.CreateTemporay(def);
            if (count == 0) return;

            // insert into destination-table, sync-table, mapping-table.
            using (var trn = DbExecutor.Connection.BeginTransaction())
            {
                var versionNo = DbExecutor.InsertVersionTable(def);
                var n = DbExecutor.InsertDestinationTable(def);
                if (count != n) throw new InvalidOperationException($"destinaition-table insert fail.(expect count:{count}, actual:{n}");

                n = DbExecutor.InsertSyncTable(def, versionNo);
                if (count != n) throw new InvalidOperationException($"sync-table insert fail.(expect count:{count}, actual:{n}");

                n = DbExecutor.InsertMappingTable(def);
                if (count != n) throw new InvalidOperationException($"mapping-table insert fail.(expect count:{count}, actual:{n}");

                trn.Commit();

                Result = new Result() { Count = count, Version = versionNo };
            }
            return;
        }
    }
}