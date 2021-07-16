using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    internal class StockDetailDatasource : CascadeDatasourceMap
    {
        public override string DestinationTableName => "stock_detail";

        public override string MappingName => "sales_detail";

        public override IEnumerable<string> DatasourceKeyColumns => new string[] { "sales_detail_id" };

        public override bool IsNeedExistsCheck => true;

        public override Func<SyncMap, string> DatasourceQueryGenarator => (def) => $@"
with
datasource as (
    select
        h.sales_date
        , d.product
        , d.amount
        , d.sales_detail_id --mapping
    from
        {def.BridgeTableName} d
        inner join sales h on d.sales_id = h.sales_id
)
";
    }
}