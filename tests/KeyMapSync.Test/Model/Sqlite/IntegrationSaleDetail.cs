using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Sqlite;

public class IntegrationSaleDetail
{
    public static Destination GetDestination()
    {
        var c = new Destination()
        {
            TableName = "integration_sale_detail",
            Sequence = new()
            {
                Column = "integration_sale_detail_id",
                Command = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0)) + row_number() over()"
            },
            Columns = new() { "integration_sale_detail_id", "sale_date", "article_name", "unit_price", "quantity", "price" },
            HasKeyMap = true,
            UseVersioning = true,
            AllowOffset = false,
        };
        return c;
    }
}

