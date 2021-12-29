using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Data;

    /// <summary>
    /// root datasource
    /// </summary>
public class Datasource : IDatasource
{
    /// <summary>
    /// name.
    /// ex.ec_sales_slip, shop_sales_slip
    /// </summary>
    public string DatasourceName { get; set; }

    /// <summary>
    /// destination table name.
    /// ex.sales_slip
    /// </summary>
    public string DestinaionTableName { get; set; }

    /// <summary>
    /// mapping name.
    /// ex.ec_sales_slip, shop_saels_slip
    /// </summary>
    public string MappingName { get; set; }

    /// <summary>
    /// datasource key column names.
    /// ex.ec_sales_slip_id
    /// </summary>
    public IEnumerable<string> DatasourceKeyColumns { get; set; }

    /// <summary>
    /// part of with query
    /// ex. with _ds as (select * from ec_sales_slip)
    /// </summary>
    public string WithQueryText { get; set; }

    /// <summary>
    /// with query alias name.
    /// </summary>
    public string AliasName { get; set; } = "_ds";

    /// <summary>
    /// sql parameter set.
    /// ex. shop_id = 1, sales_date = '2021/01/01'
    /// </summary>
    public ParameterSet ParameterSet { get; set; }

    /// <summary>
    /// extend datasoruce.
    /// </summary>
    public IList<ExtensionDatasource> Extensions { get; set; }
}

