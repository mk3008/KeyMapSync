using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class ExtensionDatasource
{
   
    /// <summary>
    /// ex."ec_shop_sales_article"
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// datasource description.
    /// </summary>

    public string Description { get; set; }
    
    public int DatasourceId { get; set; }

    /// <summary>
    /// owner datasource
    /// </summary>
    public Datasource Datasource { get; set; }

    /// <summary>
    /// ex."with _ds as (select integration_sales_slip_id, article_id from /*bridge*/)"
    /// </summary>
    public string WithQueryFormat { get; set; }

    /// <summary>
    /// ex."_ds"
    /// </summary>
    public string AliasName { get; set; }

    /// <summary>
    /// ex."ec_shop_sales_article"
    /// </summary>
    public ExtensionDestination Destination { get; set; }
}

