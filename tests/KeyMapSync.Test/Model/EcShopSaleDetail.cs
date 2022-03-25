using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model
{
    internal class EcShopSaleDetail
    {
        public static Datasource GetDatasource()
        {
            var ds = new Datasource()
            {
                DatasourceName = "ec_shop_sale_detail",
                BridgeName = "bridge_ec_shop_sale_detail",
                Destination = IntegrationSaleDetail.GetDestination(),
                Query = @"with 
ds as (
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
        inner join ec_shop_article a on sd.ec_shop_article_id = a.ec_shop_article_id
)
select * from ds",
                //Alias = "ds",
                Columns = new() { "ec_shop_sale_detail_id", "sale_date", "ec_shop_article_id", "article_name", "unit_price", "quantity", "price" },
                KeyColumns = new() { "ec_shop_sale_detail_id" },
                InspectionIgnoreColumns = new() { "ec_shop_article_id", "article_name" },
            };
            ds.Extensions.Add(GetExtensionDatasource(ds));
            ds.OffsetExtensions.Add(GetOffsetExtensionDatasource(ds));
            //ds.OffsetExtensions.Add(GetRenewalExtensionDatasource(ds));
            return ds;
        }

        private static Datasource GetExtensionDatasource(Datasource owner)
        {
            var ext = new Datasource()
            {
                Destination = ExtEcShopArtcile.GetDestination(),
                BridgeName = "bridge_ec_shop_sale_detail_ex",
                Columns = new() { "integration_sale_detail_id", "ec_shop_article_id" },
                Query = $@"
select
    integration_sale_detail_id
    , ec_shop_article_id 
from
    {owner.BridgeName}
where
    ec_shop_article_id is not null"
            };
            return ext;
        }

        private static Datasource GetOffsetExtensionDatasource(Datasource owner)
        {
            var ext = new Datasource()
            {
                Destination = ExtEcShopArtcile.GetDestination(),
                BridgeName = "bridge_ec_shop_sale_detail_ex_offset",
                Columns = new() { "integration_sale_detail_id", "ec_shop_article_id" },
                Query = $@"
select
    d.offset_integration_sale_detail_id as integration_sale_detail_id
    , e.ec_shop_article_id 
from
    {owner.BridgeName} d
    inner join integration_sale_detail_ext_ec_shop_article e on d.integration_sale_detail_id = e.integration_sale_detail_id
where
    d.offset_integration_sale_detail_id is not null
    and e.ec_shop_article_id is not null"
            };
            return ext;
        }

        private static Datasource GetRenewalExtensionDatasource(Datasource owner)
        {
            var ext = new Datasource()
            {
                Destination = ExtEcShopArtcile.GetDestination(),
                BridgeName = "bridge_ec_shop_sale_detail_ex_renew",
                Columns = new() { "integration_sale_detail_id", "ec_shop_article_id" },
                Query = $@"
select
    renewal_integration_sale_detail_id as integration_sale_detail_id
    , ec_shop_article_id 
from
    {owner.BridgeName}
where
    renewal_integration_sale_detail_id is not null
    and ec_shop_article_id is not null"
            };
            return ext;
        }
    }
}
