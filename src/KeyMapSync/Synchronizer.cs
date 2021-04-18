﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public class Synchronizer
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
                    InsertCore(def);
                    t.Commit();
                }
            }
            else
            {
                InsertCore(def);
            }
        }

        private void InsertCore(SyncMap def)
        {
            Result = null;

            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (def == null) throw new ArgumentNullException("def");

            var count = DbExecutor.CreateTemporay(def);
            if (count == 0)
            {
                Result = new Result() { Count = count };
                return;
            }

            var versionNo = DbExecutor.InsertVersionTable(def);
            var n = DbExecutor.InsertDestinationTable(def);
            if (count != n) throw new InvalidOperationException($"destinaition-table insert fail.(expect count:{count}, actual:{n}");

            n = DbExecutor.InsertSyncTable(def, versionNo);
            if (count != n) throw new InvalidOperationException($"sync-table insert fail.(expect count:{count}, actual:{n}");

            n = DbExecutor.InsertMappingTable(def);
            if (count != n) throw new InvalidOperationException($"mapping-table insert fail.(expect count:{count}, actual:{n}");

            Result = new Result() { Count = count, Version = (int?)versionNo };
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