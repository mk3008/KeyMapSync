using KeyMapSync.Validation;
using KeyMapSync.DBMS;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

/// <summary>
/// Manage data source (select query), 
/// transfer destination, 
/// and key information.
/// </summary>
public class Datasource
{
    /// <summary>
    /// Data source name.
    /// </summary>
    [Required]
    public string DatasourceName { get; set; } = String.Empty;

    /// <summary>
    /// The real table name of the data source.
    /// It is used to determine if it has been transferred.
    /// You can define a table in multiple data sources. 
    /// </summary>
    /// <example>
    /// "ec_shop_sales_detail"
    /// </example>
    [Required]
    public string TableName { get; set; } = String.Empty;

    /// <summary>
    /// Temporary table name to use when transferring data.
    /// If you use an extended data source, 
    /// this table will be the data source.
    /// </summary>
    /// <example>
    /// "ec_shop_sales_detail_bridge"
    /// </example>
    [Required]
    public string BridgeName { get; set; } = String.Empty;

    /// <summary>
    /// Select query string.
    /// </summary>
    /// <example>
    /// You can also specify a query using CTE.
    /// "select * from ec_shop_sales_detail"
    /// "with ds as (select * from ec_shop_sales_detail) select * from select * from ds"
    /// </example>
    [Required]
    public string Query { get; set; } = String.Empty;

    /// <summary>
    /// Data source forwarding destination.
    /// </summary>
    [Required]
    public Destination Destination { get; set; } = new();

    /// <summary>
    /// Data source key information.
    /// It is used to judge whether it has been transferred.
    /// </summary>
    /// <example>
    /// {"ec_shop_sales_id", "ex_shop_sales_dtail_id"}
    /// </example>
    [ListRequired]
    public List<string> KeyColumns { get; set; } = new();

    /// <summary>
    /// Data source column information. 
    /// Used during transfer. 
    /// Please define including the key column.
    /// </summary>
    /// <example>
    /// "ec_shop_sales_id", "sales_date", "article_name", "unit_price", "quantity", "price"}
    /// </example>
    [ListRequired]
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// A list of columns to exclude from inspection when offsetting.
    /// </summary>
    /// <example>
    /// "article_name"
    /// </example>
    public List<string> InspectionIgnoreColumns { get; set; } = new();

    /// <summary>
    /// A list to be inspected at the time of offset.
    /// </summary>
    public IEnumerable<string> InspectionColumns => Columns.Where(x => !InspectionIgnoreColumns.Contains(x)).Where(x => !KeyColumns.Contains(x));

    /// <summary>
    /// Extended data source. 
    /// Specify this when you want to send one data source to multiple destinations.
    /// </summary>
    public List<Datasource> Extensions { get; set; } = new();

    /// <summary>
    /// Extended data source for offsetting. 
    /// For one extended data source, 
    /// you need to define two extended data sources, 
    /// one for cancellation and one for correction. 
    /// No definition is required if you do not need to support offset forwarding.
    /// </summary>
    public List<Datasource> OffsetExtensions { get; set; } = new();

    /// <summary>
    /// Convert to a group of control tables.
    /// </summary>
    /// <returns></returns>
    public List<DbTable> ToSystemDbTables()
    {
        var lst = new List<DbTable>();

        if (Destination.KeyMapConfig != null) lst.Add(Destination.KeyMapConfig.ToDbTable(this));
        if (Destination.VersioningConfig != null)
        {
            var config = Destination.VersioningConfig;
            lst.Add(config.SyncConfig.ToDbTable(this, config));
            lst.Add(config.VersionConfig.ToDbTable(this, config));
        }
        if (Destination.KeyMapConfig?.OffsetConfig != null) lst.Add(Destination.KeyMapConfig.OffsetConfig.ToDbTable(this));

        return lst;
    }
}


