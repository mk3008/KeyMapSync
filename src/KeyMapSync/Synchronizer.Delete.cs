using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public partial class Synchronizer
    {
        public void DeleteByDestinationId(SyncMap def, int destinationId, IDbTransaction trn = null)
        {
            // argument, property check
            if (def == null) throw new ArgumentNullException("def");

            // delete destination-table, sync-table, mapping-table.
            if (trn == null)
            {
                using (var t = DbExecutor.Connection.BeginTransaction())
                {
                    DeleteByDestinationIdCore(def, destinationId);
                    t.Commit();
                }
            }
            else
            {
                DeleteByDestinationIdCore(def, destinationId);
            }
        }

        private void DeleteByDestinationIdCore(SyncMap def, int destinationId)
        {
            Result = null;

            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (def == null) throw new ArgumentNullException("def");

            var count = DbExecutor.DeleteDestinationTableByDestinationId(def, destinationId);
            if (count != 0)
            {
                DbExecutor.DeleteMappingTableByDestinationId(def, destinationId);
                DbExecutor.DeleteSyncTableByDestinationId(def, destinationId);
            }

            Result = new Result() { Count = count };
        }

        public void DeleteByVersionId(SyncMap def, int versionId, IDbTransaction trn = null)
        {
            // argument, property check
            if (def == null) throw new ArgumentNullException("def");

            // delete destination-table, sync-table, mapping-table.
            if (trn == null)
            {
                using (var t = DbExecutor.Connection.BeginTransaction())
                {
                    DeleteByVersionIdCore(def, versionId);
                    t.Commit();
                }
            }
            else
            {
                DeleteByVersionIdCore(def, versionId);
            }
        }

        private void DeleteByVersionIdCore(SyncMap def, int versionId)
        {
            Result = null;

            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (def == null) throw new ArgumentNullException("def");

            var count = DbExecutor.DeleteDestinationTableByVersionId(def, versionId);
            if (count != 0)
            {
                DbExecutor.DeleteMappingTableByVersionId(def, versionId);
                DbExecutor.DeleteSyncTableByVersionId(def, versionId);
            }

            Result = new Result() { Count = count };
        }
    }
}