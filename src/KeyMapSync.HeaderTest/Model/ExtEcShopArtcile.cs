using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.HeaderTest.Model;

public class ExtEcShopArtcile
{
    public static Destination GetDestination()
    {
        var c = new Destination()
        {
            DestinationTableName = "integration_sale_detail_ext_ec_shop_article",
            Sequence = new Sequence()
            {
                Column = "extension_id",
                Command = "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail_ext_ec_shop_article' union all select 0)) + row_number() over()"
            },
            Columns = new[] { "integration_sale_detail_id", "ec_shop_article_id" }.ToList(),
        };
        return c;
    }
}

