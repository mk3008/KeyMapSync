using Dapper;
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
        public void Offset(ITableDatasourceMap originmap, IValidateOption opt, long version = 1, IDbTransaction trn = null)
        {
            // argument check
            if (opt == null) throw new InvalidOperationException("'opt' property is null.");

            var origindef = Builder.Build(originmap);
            var def = Builder.ConvertToOffset(origindef, opt, version);

            Insert(def, trn);

            DbExecutor.OffsetMapping(origindef);
        }

        public void Offset(IDatasourceMap originmap, IValidateOption opt, long version = 1, IDbTransaction trn = null)
        {
            // argument check
            if (opt == null) throw new InvalidOperationException("'opt' property is null.");

            var origindef = Builder.Build(originmap);
            var def = Builder.ConvertToOffset(origindef, opt, version);

            Insert(def, trn);

            DbExecutor.OffsetMapping(origindef);
        }
    }
}