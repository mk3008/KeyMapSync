using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class Datasource
{
    /// <summary>
    /// ex."ec_shop_sales_detail"
    /// </summary>
    public string Name { get; set; } = String.Empty;

    /// <summary>
    /// datasource description.
    /// ex."transfrom 'ec_shop_sales_detail' to 'integration_sale_detail'." 
    /// </summary>
    //public string Description { get; set; } = String.Empty;

    /// <summary>
    /// ex."with ds as (select * from ec_shop_sales_detail) select * from select * from ds"
    /// </summary>
    public string Query { get; set; } = String.Empty;

    /// <summary>
    /// ex.
    /// {
    /// "DestinationName":"integration_sale_detail"
    /// "SequenceKeyColumn":"integration_sale_detail_id",
    /// "SequenceCommand":"(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale_detail' union all select 0))",
    /// }
    /// </summary>
    public Destination Destination { get; set; } = new();

    /// <summary>
    /// datasource key column names.
    /// ex.
    /// "ec_shop_sales_id, ex_shop_sales_dtail_id"
    /// </summary>
    public List<string> KeyColumns { get; set; } = new();

    /// <summary>
    /// ex.
    /// "sales_date, article_name, unit_price, quantity, price"
    /// </summary>
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// ex
    /// "article_name"
    /// </summary>
    public List<string> InspectionIgnoreColumns { get; set; } = new();

    public IEnumerable<string> InspectionColumns => Columns.Where(x => !InspectionIgnoreColumns.Contains(x)).Where(x => !KeyColumns.Contains(x));

    public List<ExtensionDatasource> Extensions { get; set; } = new();

    /// <summary>
    /// keymap table name format.
    /// </summary>
    public string KeyMapFormat { get; set; } = "{0}__map_{1}";

    /// <summary>
    /// ex
    /// "integration_sale_detail__map_ec_shop_sales_detail"
    /// </summary>
    public string KeyMapName => string.Format(KeyMapFormat, Destination.DestinationName, Name);
}


