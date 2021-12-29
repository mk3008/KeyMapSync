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
            var dst = new Destination()
            {
                DestinationName = "integration_sale_detail",
                SequenceKeyColumn = "integration_sale_detail_id",
                SequenceCommand = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sales_detail' union all select 0))",
                Columns = new[] { "integration_sale_detail_id", "sales_date", "article_name", "unit_price", "quantity", "price", "sync_timestamp" },
            };

            var ds = new Datasource()
            {
                Name = "ec_shop_sale_detail",
                Destination = dst,
                WithQuery = @"with 
ds as (
    select
          sd.ec_shop_sale_detail_id
        , s.sale_date
        , sd.ec_shop_article_id
        , a.article_name
        , sd.unit_price
        , sd.quantity
        , sd.price
        , current_timestamp as sync_timestamp
    from
        ec_shop_sale_detail sd
        inner join ec_shop_sale s on sd.ec_shop_sale_id = s.ec_shop_sale_id
        inner join ec_shop_article a on sd.ec_shop_article_id = a.ec_shop_article_id
)",
                Alias = "ds",
                Columns = new[] { "ec_shop_sale_detail_id", "sale_date", "ec_shop_article_id", "article_name", "unit_price", "quantity", "price", "create_timestamp" },
                KeyColumns = new[] { "ec_shop_sale_detail_id" },
                SingInversionColumns = new[] { "quantity", "price" },
                InspectionIgnoreColumns = new[] { "create_timestamp" },
            };
            return ds;
        }
    }
}
