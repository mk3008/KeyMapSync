using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Manage the conversion of transferred data and version data.
/// </summary>
public class SyncConfig
{
    /// <summary>
    /// Table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__sync";
}
