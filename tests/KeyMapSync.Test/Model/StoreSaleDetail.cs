﻿using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model
{
    internal class StoreSaleDetail
    {
        public static Datasource GetDatasource()
        {
            var ds = new Datasource()
            {
                Name = "store_sale_detail",
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

                Columns = new[] { "store_sale_detail_id", "sale_date", "store_article_id", "article_name", "unit_price", "quantity", "price", "remarks" },
                KeyColumns = new[] { "store_sale_detail_id" },
                SingInversionColumns = new[] { "quantity", "price" },
                InspectionIgnoreColumns = new[] { "store_article_id", "article_name", "remarks" },
            };
            ds.Extensions.Add(GetExtensionDatasource());
            return ds;
        }

        private static ExtensionDatasource GetExtensionDatasource()
        {
            var ext = new ExtensionDatasource()
            {
                Destination = ExtSroteSaleDetaiil.GetDestination(),
                QueryFormat = @"
select
    integration_sale_detail_id
    , store_article_id
    , remarks
from
    {0}
where
    store_article_id is not null"
            };
            return ext;
        }
    }
}
