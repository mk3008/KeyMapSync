using KeyMapSync.Validation;
using KeyMapSync.DBMS;
using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Difference transfer settings.
/// </summary>
public class OffsetConfig
{
    /// <summary>
    /// A group of columns whose signs need to be inverted when offsetting.
    /// </summary>
    /// <example>
    /// {"quantity", "price"}
    /// </example>
    [ListRequired]
    public List<string> SignInversionColumns { get; set; } = new();

    /// <summary>
    /// Table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__offset";

    /// <summary>
    /// Column prefix for offsetting.
    /// </summary>
    [Required]
    public string OffsetColumnPrefix { get; set; } = "offset_";

    /// <summary>
    /// Redesigned column prefix.
    /// </summary>
    [Required]
    public string RenewalColumnPrefix { get; set; } = "renewal_";

    /// <summary>
    /// A column that records the reasons for offsetting.
    /// </summary>
    [Required]
    public string OffsetRemarksColumn { get; set; } = "offset_remarks";
}
