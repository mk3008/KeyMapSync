﻿using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Manage transfer information.
/// </summary>
public class VersionConfig
{
    /// <summary>
    /// Table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__version";

    /// <summary>
    /// Datasourcename column name.
    /// </summary>
    [Required]
    public string DatasourceNameColumn { get; set; } = "datasource_name";

    /// <summary>
    /// Transfer settings.
    /// </summary>
    [Required]
    public string BridgeCommandColumn { get; set; } = "bridge_query";

    /// <summary>
    /// Timestamp column name.
    /// </summary>
    [Required]
    public string TimestampColumn { get; set; } = "create_at";
}
