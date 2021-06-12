using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public class Synchronizer
    {
        //public Synchronizer(DbExecutor executor)
        //{
        //    DbExecutor = executor;
        //}

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

        public void Insert(ITableDatasourceMap map, IDbTransaction trn = null)
        {
            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (Builder == null) throw new InvalidOperationException("'Builder' property is null.");
            if (map == null) throw new ArgumentNullException("map");

            var def = Builder.Build(map);
            Insert(def, trn);
        }

        public void Insert(IDatasourceMap map, IDbTransaction trn = null)
        {
            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (Builder == null) throw new InvalidOperationException("'Builder' property is null.");
            if (map == null) throw new ArgumentNullException("map");

            var def = Builder.Build(map);
            Insert(def, trn);
        }

        public void Insert(SyncMap def, IDbTransaction trn = null)
        {
            // argument, property check
            if (def == null) throw new ArgumentNullException("def");

            // insert into destination-table, sync-table, mapping-table.
            if (trn == null)
            {
                using (var t = DbExecutor.Connection.BeginTransaction())
                {
                    Result = InsertMain(def);
                    t.Commit();
                }
            }
            else
            {
                Result = InsertMain(def);
            }
        }

        private Result InsertMain(SyncMap def)
        {
            var r = InsertCore(def);

            if (r.Count == 0) return r;

            // cascade
            foreach (var item in def.DatasourceMap.Cascades)
            {
                var d = Builder.Build(item, def);
                var ir = InsertMain(d);
                r.InnerResults.Add(ir);
            }

            return r;
        }

        private Result InsertCore(SyncMap def)
        {
            Result = null;

            var sw = new Stopwatch();
            sw.Start();

            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (def == null) throw new ArgumentNullException("def");

            var count = DbExecutor.CreateTemporayOfDefault(def);
            if (count == 0)
            {
                sw.Stop();
                return new Result() { Definition = def, Count = count, Elapsed = sw.Elapsed };
            }

            var versionNo = DbExecutor.InsertVersionTableOrDefault(def);

            var n = DbExecutor.InsertDestinationTable(def);
            if (count != n) throw new InvalidOperationException($"destinaition-table insert fail.(expect count:{count}, actual:{n}");

            if (versionNo.HasValue)
            {
                n = DbExecutor.InsertSyncTable(def, versionNo.Value);
                if (count != n) throw new InvalidOperationException($"sync-table insert fail.(expect count:{count}, actual:{n}");
            }

            if (def.MappingTable != null)
            {
                n = DbExecutor.InsertMappingTable(def);
                if (count != n) throw new InvalidOperationException($"mapping-table insert fail.(expect count:{count}, actual:{n}");
            }

            sw.Stop();
            return new Result() { Definition = def, Count = count, Version = versionNo, Elapsed = sw.Elapsed };
        }

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