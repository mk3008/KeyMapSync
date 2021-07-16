using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace KeyMapSync
{
    public partial class Synchronizer
    {
        /// <summary>
        /// Validates the data since the specified synchronous version.
        /// Any changes will be offset and returned to the out-of-sync state.
        /// </summary>
        /// <param name="map"></param>
        /// <param name="opt">validate option</param>
        /// <param name="version">validate this sync version or later</param>
        /// <param name="trn"></param>
        public void Offset(ITableDatasourceMap map, IValidateOption opt, long version = 1, IDbTransaction trn = null)
        {
            // argument check
            if (opt == null) throw new InvalidOperationException("'opt' property is null.");

            var origindef = Builder.Build(map);
            var def = Builder.ConvertToOffset(origindef, opt, version);

            Insert(def, trn);

            DbExecutor.OffsetMapping(origindef);
        }

        /// <summary>
        /// Validates the data since the specified synchronous version.
        /// Any changes will be offset and returned to the out-of-sync state.
        /// </summary>
        /// <param name="map"></param>
        /// <param name="opt"></param>
        /// <param name="version">validate this sync version or later</param>
        /// <param name="trn"></param>
        public void Offset(IDatasourceMap map, IValidateOption opt, long version = 1, IDbTransaction trn = null)
        {
            // argument check
            if (opt == null) throw new InvalidOperationException("'opt' property is null.");

            var def = Builder.Build(map);

            if (trn == null)
            {
                using (var t = DbExecutor.Connection.BeginTransaction())
                {
                    OffsetMain(def, opt, version, t);
                    t.Commit();
                }
            }
            else
            {
                OffsetMain(def, opt, version, trn);
            }
        }

        private void OffsetMain(SyncMap def, IValidateOption opt, long version, IDbTransaction trn)
        {
            //offset insert.
            OffsetCore(def, opt, version, trn);

            //delete origin map.
            if (def.MappingTable != null) DbExecutor.OffsetMapping(def);

            //foreach (var item in def.DatasourceMap.Cascades.Where((x) => x.IsUpperCascade))
            //{
            //    //delete upper map
            //    var d = Builder.Build(item);
            //    DbExecutor.OffsetMapping(d);
            //}
        }

        private void OffsetCore(SyncMap origindef, IValidateOption opt, long version, IDbTransaction trn)
        {
            var def = Builder.ConvertToOffset(origindef, opt, version);

            //offset insert.
            Insert(def, trn);
        }
    }
}