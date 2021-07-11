using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    internal class SalesDetailDatasource : RootDatasourceMap
    {
        public SalesDetailDatasource()
        {
            //cascade datasource
            Cascades.Add(new SalesDetailExtensionRemarks());
            Cascades.Add(new SalesDatasource());
            Cascades.Add(new StockDetailDatasource());
        }

        public override string DestinationTableName => "sales_detail";

        public override string MappingName => "sales_data";

        public override IEnumerable<string> DatasourceKeyColumns => new string[] { "sales_data_seq" };

        public override bool IsNeedExistsCheck => true;

        public override string DatasourceQuery => $@"
with
sales_data_ds as (
    select
        *
    from
        sales_data d
),
header_ds as (
    select
        sales_data_id
        , sales_date
        , row_number() over() + (select max(seq) from (select seq from sqlite_sequence where name = 'sales' union all select 0)) as sales_id
    from
        (
            select distinct
                sales_data_id
                , sales_date
            from
                sales_data_ds
        ) q
),
datasource as (
    select
        h.sales_id
        , h.sales_date
        , d.product
        , d.amount
        , d.price
        , d.remarks
        , d.sales_data_seq
    from
        sales_data_ds as d
        inner join header_ds as h on d.sales_data_id = h.sales_data_id and d.sales_date = h.sales_date
)
";
    }
}