using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model;

public class IntegrationSaleDetail
{
    public static Destination GetDestination()
    {
        var c = new Destination()
        {
            DestinationName = "integration_sale_detail",
            SequenceKeyColumn = "integration_sale_detail_id",
            SequenceCommand = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sales_detail' union all select 0)) + row_number() over()",
            Columns = new[] { "integration_sale_detail_id", "sales_date", "article_name", "unit_price", "quantity", "price" },
            VersionSequenceCommand = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sales_detail__version' union all select 0)) + 1"
        };
        return c;
    }
}

