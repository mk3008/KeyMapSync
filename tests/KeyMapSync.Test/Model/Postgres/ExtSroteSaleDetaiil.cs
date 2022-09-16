﻿using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Postgres;

public class ExtSroteSaleDetaiil
{
    public static Destination GetDestination()
    {
        var c = new Destination()
        {
            TableName = "integration_sale_detail_ext_store_sale_detail",
            Sequence = new Sequence()
            {
                Column = "extension_id",
                Command = "nexval('integration_sale_detail_ext_store_sale_detail_extension_id_seq')"
            },
            Columns = new[] { "extension_id", "integration_sale_detail_id", "store_article_id", "remarks" }.ToList(),
        };
        return c;
    }
}

