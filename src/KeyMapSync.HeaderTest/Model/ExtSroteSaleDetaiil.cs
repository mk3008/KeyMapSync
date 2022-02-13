using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.HeaderTest.Model;

public class ExtSroteSaleDetaiil
{
    public static ExtensionDestination GetDestination()
    {
        var c = new ExtensionDestination()
        {
            DestinationName = "integration_sale_detail_ext_store_sale_detail",
            Columns = new[] { "store_article_id", "remarks" },
        };
        return c;
    }
}

