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
        public void Offset(SyncMap def, IEnumerable<string> valuesColumns, int destinationId, IDbTransaction trn = null)
        {
            // argument, property check
            if (def == null) throw new ArgumentNullException("def");

            // delete destination-table, sync-table, mapping-table.
            if (trn == null)
            {
                using (var t = DbExecutor.Connection.BeginTransaction())
                {
                    Offset(t, def, valuesColumns, destinationId);
                    t.Commit();
                }
            }
            else
            {
                Offset(trn, def, valuesColumns, destinationId);
            }
        }

        private void Offset(IDbTransaction trn, SyncMap def, IEnumerable<string> valueColumns, int destinationId)
        {
            Result = null;

            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (def == null) throw new ArgumentNullException("def");

            // build offset sync definition
            var offsetDef = def.ToOffsetSyncMap(Builder, valueColumns, destinationId);
            this.Insert(offsetDef, trn);

            if (this.Result.Count != 0)
            {
                DbExecutor.DeleteMappingTableByDestinationId(def, destinationId);
                DbExecutor.DeleteSyncTableByDestinationId(def, destinationId);
            }
        }

        public void OffsetVersion(SyncMap def, IEnumerable<string> valuesColumns, int versionId, IDbTransaction trn = null)
        {
            // argument, property check
            if (def == null) throw new ArgumentNullException("def");

            // delete destination-table, sync-table, mapping-table.
            if (trn == null)
            {
                using (var t = DbExecutor.Connection.BeginTransaction())
                {
                    OffsetVersion(t, def, valuesColumns, versionId);
                    t.Commit();
                }
            }
            else
            {
                OffsetVersion(trn, def, valuesColumns, versionId);
            }
        }

        private void OffsetVersion(IDbTransaction trn, SyncMap def, IEnumerable<string> valueColumns, int versionId)
        {
            Result = null;

            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (def == null) throw new ArgumentNullException("def");

            // build offset sync definition
            var offsetDef = def.ToOffsetVersionSyncMap(Builder, valueColumns, versionId);
            this.Insert(offsetDef, trn);

            if (this.Result.Count != 0)
            {
                DbExecutor.DeleteMappingTableByVersionId(def, versionId);
                DbExecutor.DeleteSyncTableByVersionId(def, versionId);
            }
        }
    }
}