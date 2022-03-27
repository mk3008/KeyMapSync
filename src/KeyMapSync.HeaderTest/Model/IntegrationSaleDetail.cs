using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.HeaderTest.Model;

public class IntegrationSaleDetail
{
    public static Destination GetDestination()
    {
        var grp = new GroupDestination()
        {
            TableName = "integration_sale",
            Sequence = new Sequence()
            {
                Column = "integration_sale_id",
                Command = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale' union all select 0)) + row_number() over()"
            },
            Columns = { "integration_sale_id", "shop_id", "sale_date" },
        };

        var c = new Destination()
        {
            TableName = "integration_sale_detail",
            Sequence = new Sequence
            {
                Column = "integration_sale_detail_id",
                Command = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over()"
            },
            Columns = new() { "integration_sale_detail_id", "integration_sale_id", "article_name", "unit_price", "quantity", "price" },
            KeyMapConfig = new()
            {
                OffsetConfig = new()
                {
                    SignInversionColumns = new() { "quantity", "price" },
                }
            },
            VersioningConfig = new() { 
                Sequence = new Sequence()
                {
                    Column = "version_id",
                    Command = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail__version' union all select 0)) + 1",
                }
            }
        };

        c.Groups.Add(grp);

        return c;
    }
}

