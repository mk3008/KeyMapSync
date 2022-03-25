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

public class Datasource
{
    /// <summary>
    /// ex."ec_shop_sales_detail"
    /// </summary>
    [Required]
    public string DatasourceName { get; set; } = String.Empty;

    /// <summary>
    /// Temporary table name.
    /// ex."ec_shop_sales_detail_bridge"
    /// </summary>
    [Required]
    public string BridgeName { get; set; } = String.Empty;

    /// <summary>
    /// Query string.
    /// ex."with ds as (select * from ec_shop_sales_detail) select * from select * from ds"
    /// </summary>
    [Required]
    public string Query { get; set; } = String.Empty;

    /// <summary>
    /// Destination
    /// </summary>
    [Required]
    public Destination Destination { get; set; } = new();

    /// <summary>
    /// datasource key column names.
    /// ex.
    /// "ec_shop_sales_id, ex_shop_sales_dtail_id"
    /// </summary>
    [ListRequired]
    public List<string> KeyColumns { get; set; } = new();

    /// <summary>
    /// ex.
    /// "ec_shop_sales_id, ex_shop_sales_dtail_id, sales_date, article_name, unit_price, quantity, price"
    /// </summary>
    [ListRequired]
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// ex
    /// "article_name"
    /// </summary>
    public List<string> InspectionIgnoreColumns { get; set; } = new();

    public IEnumerable<string> InspectionColumns => Columns.Where(x => !InspectionIgnoreColumns.Contains(x)).Where(x => !KeyColumns.Contains(x));

    public List<Datasource> Extensions { get; set; } = new();

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


