using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Sqlite
{
    internal class StoreSaleDetail
    {
        public static Datasource GetDatasource()
        {
            var ds = new Datasource()
            {
                TableName = "store_sale_detail",
                Destination = IntegrationSaleDetail.GetDestination(),
                Query = @"
select
    ssd.store_sale_detail_id
    , ss.sale_date
    , ssd.store_article_id
    , sa.article_name
    , ssd.unit_price
    , ssd.quantity
    , ssd.price
    , ssd.remarks
from
    store_sale_detail ssd
    inner join store_sale ss on ssd.store_sale_id = ss.store_sale_id
    inner join store_article sa on ssd.store_article_id = sa.store_article_id",
                InspectionIgnoreColumns = new() { "store_article_id", "article_name", "remarks" },
            };
            ds.KeyColumns.Add("store_sale_detail_id", DBMS.DbColumn.Types.Numeric);
            ds.Extensions.Add(GetExtensionDatasource());
            return ds;
        }

        private static Datasource GetExtensionDatasource()
        {
            var ext = new Datasource()
            {
                Destination = ExtSroteSaleDetaiil.GetDestination(),
                Query = $@"
select
    integration_sale_detail_id
    , store_article_id
    , remarks
from
    bridge
where
    store_article_id is not null"
            };
            return ext;
        }
    }
}
