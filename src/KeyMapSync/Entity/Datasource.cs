using KeyMapSync.Validation;
using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static KeyMapSync.DBMS.DbColumn;

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
    public Dictionary<string, Types> KeyColumns { get; set; } = new();

    /// <summary>
    /// A list of columns to exclude from inspection when offsetting.
    /// </summary>
    /// <example>
    /// "article_name"
    /// </example>
    public List<string> InspectionIgnoreColumns { get; set; } = new();

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

    public string? GetKeymapTableName()
    {
        var config = Destination.KeyMapConfig;
        if (config == null) return null;
        return string.Format(config.TableNameFormat, Destination.TableName, TableName);
    }

    public string? GetSyncTableName()
    {
        var config = Destination.VersioningConfig?.SyncConfig;
        if (config == null) return null;
        return string.Format(config.TableNameFormat, Destination.TableName);
    }

    public string? GetVersionTableName()
    {
        var config = Destination.VersioningConfig?.VersionConfig;
        if (config == null) return null;
        return string.Format(config.TableNameFormat, Destination.TableName);
    }

    public string? GetOffsetTableName()
    {
        var config = Destination.KeyMapConfig?.OffsetConfig;
        if (config == null) return null;
        return string.Format(config.TableNameFormat, Destination.TableName);
    }

    public string? GetOffsetColumnName()
    {
        var config = Destination.KeyMapConfig?.OffsetConfig;
        if (config == null) return null;
        return $"{config.OffsetColumnPrefix}{Destination.Sequence.Column}";
    }

    public string? GetRenewalColumnName()
    {
        var config = Destination.KeyMapConfig?.OffsetConfig;
        if (config == null) return null;
        return $"{config.RenewalColumnPrefix}{Destination.Sequence.Column}";
    }
}
