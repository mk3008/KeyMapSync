using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Version control settings.
/// </summary>
public class VersioningConfig
{
    /// <summary>
    /// Version sequence.
    /// </summary>
    [Required]
    public Sequence Sequence { get; set; } = new() { Column = "version_id" };

    /// <summary>
    /// Version settings and data association settings.
    /// </summary>


    /// <summary>
    /// Version setting.
    /// </summary>
    [Required]
    public VersionConfig VersionConfig { get; set; } = new();
}
