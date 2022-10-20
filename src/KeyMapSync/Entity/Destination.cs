using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// It manages table names, columns, 
/// and sequence information of the transfer destination table. 
/// You can also specify how to manage the transfer.
/// </summary>
public class Destination
{
    public long DestinationId { get; set; }

    [Required]
    public string DestinationName { get; set; } = String.Empty;

    public string SchemaName { get; set; } = String.Empty;

    /// <summary>
    /// The name of the transfer destination table.
    /// </summary>
    [Required]
    public string TableName { get; set; } = String.Empty;

    public string TableFulleName => (SchemaName == string.Empty) ? TableName : $"{SchemaName}.{TableName}";

    public string Description { get; set; } = string.Empty;

    /// <summary>
    /// If the destination has a group header table, specify it.
    /// </summary>
    //public List<GroupDestination> Groups { get; set; } = new();

    /// <summary>
    /// Sequence information of the transfer destination table.
    /// </summary>
    [Required]
    public Sequence SequenceConfig { get; set; } = new();

    /// <summary>
    /// A group of columns in the destination table. 
    /// Please define including the sequence.
    /// </summary>
    //[ListRequired]
    public string[] Columns { get; set; } = Array.Empty<string>();

    /// <summary>
    /// A group of columns whose signs need to be inverted when offsetting.
    /// </summary>
    /// <example>
    /// {"quantity", "price"}
    /// </example>
    //[ListRequired]
    public string[] SignInversionColumns { get; set; } = Array.Empty<string>();

    /// <summary>
    /// A list of columns to exclude from inspection when offsetting.
    /// </summary>
    /// <example>
    /// "article_name"
    /// </example>
    //[ListRequired]
    public string[] InspectionIgnoreColumns { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Gets the columns of the transfer destination table excluding the sequence columns.
    /// </summary>
    /// <returns></returns>
    public List<string> GetInsertColumns() => Columns.Where(x => x != SequenceConfig.Column).ToList();

    public bool AllowOffset { get; set; } = true;

    ///// <summary>
    ///// Specify if you want to reverse-lookup the source data source. 
    ///// Also, specify if you want to perform differential transfer.
    ///// Also, specify if you want to check the difference in the transferred data.
    ///// </summary>
    //public KeyMapConfig KeyMapConfig { get; set; } = new();

    //[Required]
    //public SyncConfig SyncConfig { get; set; } = new();

    //private OffsetConfig? _offsetConfig = null;

    ///// <summary>
    ///// Difference check settings. 
    ///// Specify if you want to perform difference transfer.
    ///// </summary>
    //public OffsetConfig? OffsetConfig
    //{
    //    get
    //    {
    //        if (!AllowOffset) return null;
    //        _offsetConfig ??= new OffsetConfig();
    //        return _offsetConfig;
    //    }
    //    set
    //    {
    //        _offsetConfig = value;
    //    }
    //}
}