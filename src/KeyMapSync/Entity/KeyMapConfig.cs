using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Manages key information conversion between transfer source and transfer destination. 
/// Used for difference transfer, 
/// difference check, 
/// and reverse lookup. 
/// Since the basic settings are specified by default, 
/// they can usually be used simply by declaring them.
/// </summary>
public class KeyMapConfig
{
    /// <summary>
    /// KeyMa table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__map_{1}";
}