using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.ModelHeaderDetail;

public class IntegrationSaleDetail
{
    public static Destination GetDestination()
    {
        var grp = new GroupDestination()
        {
            GroupDestinationId = 1,
            GroupDestinationName = "integration_sale",
            GroupColumns = new[] { "shop_id", "sale_date" }.ToList(),
            SequenceKeyColumn = "integration_sale_id",
            SequenceCommand = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale' union all select 0)) + row_number() over()",
        };

        var c = new Destination()
        {
            DestinationName = "integration_sale_detail",
            SequenceKeyColumn = "integration_sale_detail_id",
            SequenceCommand = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over()",
            Columns = new[] { "integration_sale_detail_id", "sale_date", "article_name", "unit_price", "quantity", "price" },
            SingInversionColumns = new[] { "quantity", "price" },
            VersionSequenceCommand = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail__version' union all select 0)) + 1",
        };
        c.Groups.Add(grp);

        return c;
    }
}

