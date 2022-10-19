using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Group header forwarding destination.
/// </summary>
public class GroupDestination
{
    /// <summary>
    /// The name of the transfer destination table.
    /// </summary>
    [Required]
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Sequence information of the transfer destination table.
    /// </summary>
    [Required]
    public Sequence Sequence { get; set; } = new();

    /// <summary>
    /// A group of columns in the destination table. 
    /// Please define including the sequence.
    /// </summary>
    //[ListRequired]
    public List<string> Columns { get; init; } = new();
}