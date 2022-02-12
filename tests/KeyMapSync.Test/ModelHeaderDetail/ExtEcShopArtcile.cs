using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.ModelHeaderDetail;

public class ExtEcShopArtcile
{
    public static ExtensionDestination GetDestination()
    {
        var c = new ExtensionDestination()
        {
            DestinationName = "integration_sale_detail_ext_ec_shop_article",
            Columns = new[] { "integration_sale_detail_id", "ec_shop_article_id" },
        };
        return c;
    }
}

