//using KeyMapSync.Validation;
using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Difference transfer settings.
/// </summary>
public class OffsetConfig
{
    /// <summary>
    /// Table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__offset";

    /// <summary>
    /// Column prefix for offsetting.
    /// </summary>
    [Required]
    public string CancelColumnPrefix { get; set; } = "cancel_";

    /// <summary>
    /// Redesigned column prefix.
    /// </summary>
    [Required]
    public string RenewColumnPrefix { get; set; } = "renew_";

    /// <summary>
    /// A column that records the reasons for offsetting.
    /// </summary>
    [Required]
    public string OffsetRemarksColumn { get; set; } = "offset_remarks";
}
