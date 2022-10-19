﻿//using KeyMapSync.Validation;
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
