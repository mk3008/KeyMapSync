using KeyMapSync.DBMS;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    public Sequence Sequence { get; set; } = new();

    /// <summary>
    /// Version settings and data association settings.
    /// </summary>
    [Required]
    public SyncConfig SyncConfig { get; set; } = new();

    /// <summary>
    /// Version setting.
    /// </summary>
    [Required]
    public VersionConfig VersionConfig { get; set; } = new();

    public string ToSql() => $"select {Sequence.Command} as {Sequence.Column}";
    
    public CteQuery ToCteQuery()
    {
        var cte = new CteQuery()
        {
            AliasName = "_version",
            Query = ToSql()
        };
        return cte;
    }
}
