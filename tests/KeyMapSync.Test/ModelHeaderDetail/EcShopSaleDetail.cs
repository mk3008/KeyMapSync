using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.ModelHeaderDetail
{
    internal class EcShopSaleDetail
    {
        public static Datasource GetDatasource()
        {
            var ds = new Datasource()
            {
                Name = "ec_shop_sale_detail",
                Destination = IntegrationSaleDetail.GetDestination(),
                Query = @"with 
ds as (
    select
          sd.ec_shop_sale_detail_id
        , s.shop_id
        , s.sale_date
        , sd.ec_shop_article_id
        , a.article_name
        , sd.unit_price
        , sd.quantity
        , sd.price
    from
        ec_shop_sale_detail sd
        inner join ec_shop_sale s on sd.ec_shop_sale_id = s.ec_shop_sale_id
        inner join ec_shop_article a on sd.ec_shop_article_id = a.ec_shop_article_id
)
select * from ds",
                //Alias = "ds",
                Columns = new[] { "ec_shop_sale_detail_id", "shop_id", "sale_date", "ec_shop_article_id", "article_name", "unit_price", "quantity", "price" },
                KeyColumns = new[] { "ec_shop_sale_detail_id" },
                InspectionIgnoreColumns = new[] { "ec_shop_article_id", "article_name" },
            };
            ds.Extensions.Add(GetExtensionDatasource());
            return ds;
        }

        private static ExtensionDatasource GetExtensionDatasource()
        {
            var ext = new ExtensionDatasource()
            {
                Destination = ExtEcShopArtcile.GetDestination(),
                QueryFormat = @"
select
    integration_sale_detail_id
    , ec_shop_article_id 
from
    {0}
where
    ec_shop_article_id is not null"
            };
            return ext;
        }
    }
}
