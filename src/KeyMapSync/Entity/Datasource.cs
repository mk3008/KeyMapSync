using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;
using static KeyMapSync.DBMS.DbColumn;

namespace KeyMapSync.Entity;

/// <summary>
/// Manage data source (select query), 
/// transfer destination, 
/// and key information.
/// </summary>
public class Datasource
{
    public long DatasourceId { get; set; }

    public string DatasourceName { get; set; } = string.Empty;

    public long DestinationId { get; set; }

    /// <summary>
    /// Data source forwarding destination.
    /// </summary>
    [JsonIgnore]
    public Destination Destination { get; set; } = new();

    public string GroupName { get; set; } = String.Empty;

    [Required]
    public string SchemaName { get; set; } = String.Empty;

    /// <summary>
    /// Data source name.
    /// </summary>
    [Required]
    public string TableName { get; set; } = String.Empty;

    public string TableFulleName => (SchemaName == string.Empty) ? TableName : $"{SchemaName}.{TableName}";

    public string Description { get; set; } = string.Empty;

    public bool Disable { get; set; } = false;

    /// <summary>
    /// The real table name of the data source.
    /// It is used to determine if it has been transferred.
    /// You can define a table in multiple data sources. 
    /// </summary>
    /// <example>
    /// "ec_shop_sales_detail"
    /// </example>
    [Required]
    public string MapName { get; set; } = String.Empty;

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
    /// Data source key information.
    /// It is used to judge whether it has been transferred.
    /// </summary>
    /// <example>
    /// {"ec_shop_sales_id", "ex_shop_sales_dtail_id"}
    /// </example>
    //[ListRequired]
    public Dictionary<string, Types> KeyColumnsConfig { get; set; } = new();

    /// <summary>
    /// Extended data source. 
    /// Specify this when you want to send one data source to multiple destinations.
    /// </summary>
    [JsonIgnore]
    public List<Datasource> Extensions { get; set; } = new();

    public long[] ExtensionDatasourceIds { get; set; } = Array.Empty<long>();

    public bool IsRoot => (string.IsNullOrEmpty(MapName)) ? false : true;

    public bool HasKeymap => (string.IsNullOrEmpty(MapName)) ? false : true;
}
