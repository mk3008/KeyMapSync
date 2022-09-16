using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Postgres;

public class ExtEcShopArtcile
{
    public static Destination GetDestination()
    {
        var c = new Destination()
        {
            TableName = "integration_sale_detail_ext_ec_shop_article",
            Sequence = new Sequence()
            {
                Column = "extension_id",
                Command = "nextval('integration_sale_detail_ext_ec_shop_article_extension_id_seq')"
            },
            Columns = new[] { "extension_id", "integration_sale_detail_id", "ec_shop_article_id" }.ToList(),
        };
        return c;
    }
}

