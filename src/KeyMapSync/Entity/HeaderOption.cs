using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class HeaderOption
{
    /// <summary>
    /// id.
    /// </summary>
    public int? HeaderOptionId { get; set; }

    /// <summary>
    /// ex."integration_sales_slip header"
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// datasource description.
    /// </summary>

    public string Description { get; set; }

    //public int DatasourceId { get; set; }

    /// <summary>
    /// owner datasource
    /// </summary>
    //public Datasource Datasource { get; set; }

    /// <summary>
    /// ex."ec_shop_sales_id"
    /// </summary>
    public IEnumerable<string> Columns { get; set; }

    /// <summary>
    /// ex."integration_sales_slip"
    /// </summary>
    public string HeaderDestination { get; set; }
}

