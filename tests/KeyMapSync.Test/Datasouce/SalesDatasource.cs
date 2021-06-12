using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    internal class SalesDatasource : ExtensionDatasourceMap
    {
        public override string DestinationTableName => "sales";

        public override Func<SyncMap, string> DatasourceQueryGenarator => (def) => $@"
with
datasource as (
    select distinct
        sales_id
        , sales_date
    from
        {def.DatasourceTable.TableName}
)";
    }
}