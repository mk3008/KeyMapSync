using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static KeyMapSync.DBMS.DbColumn;

namespace KeyMapSync.Test.Model
{
    internal class EcShopSaleDetail
    {
        public static Datasource GetDatasource()
        {
            var ds = new Datasource()
            {
                DatasourceName = "ec_shop_sale_detail",
                TableName = "ec_shop_sale_detail",
                Destination = IntegrationSaleDetail.GetDestination(),
                Query = @"
select
      sd.ec_shop_sale_detail_id
    , s.sale_date
    , sd.ec_shop_article_id
    , a.article_name
    , sd.unit_price
    , sd.quantity
    , sd.price
from
    ec_shop_sale_detail sd
    inner join ec_shop_sale s on sd.ec_shop_sale_id = s.ec_shop_sale_id
    inner join ec_shop_article a on sd.ec_shop_article_id = a.ec_shop_article_id",

                InspectionIgnoreColumns = new() { "ec_shop_article_id", "article_name" },
            };
            ds.KeyColumns.Add("ec_shop_sale_detail_id", Types.Numeric);
            ds.Extensions.Add(GetExtensionDatasource());

            //ds.OffsetExtensions.Add(GetOffsetExtensionDatasource(ds));

            return ds;
        }

        private static Datasource GetExtensionDatasource()
        {
            var ext = new Datasource()
            {
                DatasourceName = "extension",
                Destination = ExtEcShopArtcile.GetDestination(),
                Query = $@"
select
    integration_sale_detail_id
    , ec_shop_article_id 
from
    bridge
where
    ec_shop_article_id is not null"
            };
            return ext;
        }

        private static Datasource GetOffsetExtensionDatasource()
        {
            var ext = new Datasource()
            {
                Destination = ExtEcShopArtcile.GetDestination(),
                Query = $@"
select
    b.offset_integration_sale_detail_id as integration_sale_detail_id
    , e.ec_shop_article_id 
from
    bridge b
    inner join integration_sale_detail_ext_ec_shop_article e on b.integration_sale_detail_id = e.integration_sale_detail_id
where
    b.offset_integration_sale_detail_id is not null
    and e.ec_shop_article_id is not null"
            };
            return ext;
        }

        private static Datasource GetRenewalExtensionDatasource(Datasource owner)
        {
            var ext = new Datasource()
            {
                Destination = ExtEcShopArtcile.GetDestination(),
                Query = $@"
select
    renewal_integration_sale_detail_id as integration_sale_detail_id
    , ec_shop_article_id 
from
    bridge
where
    renewal_integration_sale_detail_id is not null
    and ec_shop_article_id is not null"
            };
            return ext;
        }
    }
}
