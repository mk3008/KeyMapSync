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
        public void Insert(IDatasourceMappable map, IDbTransaction trn = null)
        {
            if (map is ITableDatasourceMap)
            {
                Insert(map as ITableDatasourceMap, trn);
                return;
            }

            if (map is IDatasourceMap)
            {
                Insert(map as IDatasourceMap, trn);
                return;
            }

            throw new NotSupportedException();
        }

        private void Insert(ITableDatasourceMap map, IDbTransaction trn = null)
        {
            // argument, property check
            if (DbExecutor == null) throw new InvalidOperationException("'DbExecutor' property is null.");
            if (Builder == null) throw new InvalidOperationException("'Builder' property is null.");
            if (map == null) throw new ArgumentNullException("map");

            var def = Builder.Build(map);
            Insert(def, trn);
        }

        private void Insert(IDatasourceMap map, IDbTransaction trn = null)
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

            if (def.DestinationTable?.TableFullName != null && r.Count == 0) return r;

            // cascade
            foreach (var item in def.Cascades)
            {
                var ir = InsertMain(item);
                r.InnerResults.Add(ir);
            }

            DeleteMapMain(def);

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

            var count = DbExecutor.CreateBridgeOrDefault(def);
            if (count == 0 || def.DatasourceMap.IsBridge)
            {
                sw.Stop();
                return new Result() { Definition = def, Count = count, Elapsed = sw.Elapsed };
            }

            var versionNo = DbExecutor.InsertVersionTableOrDefault(def);

            var n = 0;
            if (def.DestinationTable == null || def.DestinationTable.TableFullName == null)
            {
                count = 0;
            }
            else
            {
                n = DbExecutor.InsertDestinationTable(def);
                if (count != n) throw new InvalidOperationException($"destinaition-table insert fail.(expect count:{count}, actual:{n}");

                if (versionNo.HasValue)
                {
                    n = DbExecutor.InsertSyncTable(def, versionNo.Value);
                    if (count != n) throw new InvalidOperationException($"sync-table insert fail.(expect count:{count}, actual:{n}");
                }
            }

            if (def.MappingTable != null)
            {
                n = DbExecutor.InsertMappingTable(def);
                if (count != n) throw new InvalidOperationException($"mapping-table insert fail.(expect count:{count}, actual:{n}");
            }

            sw.Stop();
            return new Result() { Definition = def, Count = count, Version = versionNo, Elapsed = sw.Elapsed };
        }

        private void DeleteMapMain(SyncMap def)
        {
            if (def.Origin != null && def.Origin.MappingTable != null)
            {
                DeleteMapCore(def.Origin);
            }

            foreach (var item in def.Cascades)
            {
                DeleteMapMain(item);
            }
        }

        private Result DeleteMapCore(SyncMap def)
        {
            Result = null;

            var sw = new Stopwatch();
            sw.Start();

            var count = DbExecutor.OffsetMapping(def);

            sw.Stop();
            return new Result() { Definition = def, Count = count, Elapsed = sw.Elapsed };
        }
    }
}