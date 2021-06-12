using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    internal class SalesDetailExtensionRemarks : ExtensionDatasourceMap
    {
        public override string DestinationTableName => "sales_detail_ext_remarks";

        public override Func<SyncMap, string> DatasourceQueryGenarator => (def) => $@"
with
datasource as (
    select
        sales_detail_id
        , remarks
    from
        {def.DatasourceTable.TableName}
    where
        remarks is not null
)";
    }
}