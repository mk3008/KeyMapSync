using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    internal class SalesDetailDatasource : CascadeDatasourceMap
    {
        public SalesDetailDatasource()
        {
            //cascade datasource
            Cascades.Add(new SalesDetailExtensionRemarks());
            Cascades.Add(new StockDetailDatasource());
        }

        public override string DestinationTableName => "sales_detail";

        public override string MappingName => "sales_data";

        public override IEnumerable<string> DatasourceKeyColumns => new string[] { "sales_data_seq" };

        public override bool IsNeedExistsCheck => true;

        public override Func<SyncMap, string> DatasourceQueryGenarator => (def) => $@"
with
datasource as (
    select
          h.sales_id
        , d.*
    from
        {def.BridgeTableName} as d
        inner join (
            select
                s.sales_id
                , s.sales_date
                , m.sales_data_id
            from
                sales s
                inner join sales_map_sales_data m on s.sales_id = m.sales_id
        ) h on d.sales_data_id = h.sales_data_id and d.sales_date = h.sales_date
)
";

        //private string GetDatasourceQuery(SyncMap def)
        //{
        //}
    }
}