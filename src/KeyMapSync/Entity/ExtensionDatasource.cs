using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class ExtensionDatasource
{
    public string Name { get; set; }

    /// <summary>
    /// owner datasource
    /// </summary>
    public Datasource Datasource { get; set; }

    /// <summary>
    /// ex."select integration_sales_slip_id, article_id from {0}/*{0} is bridge table name*/"
    /// </summary>
    public string QueryFormat { get; set; }

    /// <summary>
    /// ex."ec_shop_sales_article"
    /// </summary>
    public ExtensionDestination Destination { get; set; }
}

