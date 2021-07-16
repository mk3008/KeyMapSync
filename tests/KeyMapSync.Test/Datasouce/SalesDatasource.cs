using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    internal class SalesDatasource : CascadeDatasourceMap
    {
        public override string DestinationTableName => "sales";

        public override string MappingName => "sales_data";

        public override IEnumerable<string> DatasourceKeyColumns => new string[] { "sales_data_id" };

        public override Func<SyncMap, string> DatasourceQueryGenarator => (def) => $@"
with
datasource as (
    select distinct
        sales_date
        , sales_data_id
    from
        {def.BridgeTableName}
)";

        public override bool IsUpperCascade => true;
    }
}