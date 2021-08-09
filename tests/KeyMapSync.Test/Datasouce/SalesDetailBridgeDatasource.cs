using KeyMapSync.Test.Filter;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    internal class SalesDetailBridgeDatasource : BridgeDatasourceMap
    {
        public SalesDetailBridgeDatasource()
        {
            //cascade datasource
            Cascades.Add(new SalesDatasource());
            Cascades.Add(new SalesDetailDatasource());
        }

        public string ProductCondition { get; set; }

        public override string DestinationTableName => "sales_detail";

        public override string MappingName => "sales_data";

        public override IEnumerable<string> DatasourceKeyColumns => new string[] { "sales_data_seq" };

        public override bool IsNeedExistsCheck => true;

        public override string DatasourceQuery => $@"
with
datasource as (
    select
        --header key
          sales_data_id
        , sales_date
        --values
        , product
        , amount
        , price
        --extension
        , remarks
        --mapping key
        , sales_data_seq
    from
        sales_data
)
";

        public override Type ActualDatasourceType => typeof(SalesDetailDatasource);

        public override DatasourceFilter Filter => GetFilter();

        private DatasourceFilter GetFilter()
        {
            if (ProductCondition == null) return null;
            return new ProductFilter(ProductCondition);
        }
    }
}