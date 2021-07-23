using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace KeyMapSync
{
    public partial class Synchronizer
    {
        public void Offset(IDatasourceMappable map, IValidateOption opt, int version = 1, IDbTransaction trn = null)
        {
            if (map is ITableDatasourceMap)
            {
                Offset(map as ITableDatasourceMap, opt, version, trn);
                return;
            }

            if (map is IDatasourceMap)
            {
                Offset(map as IDatasourceMap, opt, version, trn);
                return;
            }

            throw new NotSupportedException();
        }

        /// <summary>
        /// Validates the data since the specified synchronous version.
        /// Any changes will be offset and returned to the out-of-sync state.
        /// </summary>
        /// <param name="map"></param>
        /// <param name="opt">validate option</param>
        /// <param name="version">validate this sync version or later</param>
        /// <param name="trn"></param>
        private void Offset(ITableDatasourceMap map, IValidateOption opt, int version = 1, IDbTransaction trn = null)
        {
            // argument check
            if (opt == null) throw new InvalidOperationException("'opt' property is null.");

            var def = Builder.BuildAsOffset(map, opt, version);

            Insert(def, trn);
        }

        /// <summary>
        /// Validates the data since the specified synchronous version.
        /// Any changes will be offset and returned to the out-of-sync state.
        /// </summary>
        /// <param name="map"></param>
        /// <param name="opt"></param>
        /// <param name="version">validate this sync version or later</param>
        /// <param name="trn"></param>
        private void Offset(IDatasourceMap map, IValidateOption opt, int version = 1, IDbTransaction trn = null)
        {
            // argument check
            if (opt == null) throw new InvalidOperationException("'opt' property is null.");

            var def = Builder.BuildAsOffset(map, opt, version);

            Insert(def, trn);
        }
    }
}