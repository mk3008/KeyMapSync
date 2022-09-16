using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Postgres;

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
                Command = "nextval('integration_sale_detail_integration_sale_detail_id_seq')"
            },
            Columns = new() { "integration_sale_detail_id", "sale_date", "article_name", "unit_price", "quantity", "price" },
            VersioningConfig = new()
            {
                Sequence = new()
                {
                    Column = "version_id",
                    Command = "nextval('integration_sale_detail__version_version_id_seq')"
                }
            },
            KeyMapConfig = new()
            {
                OffsetConfig = new()
                {
                    SignInversionColumns = new() { "quantity", "price" },
                }
            }
        };
        return c;
    }
}

