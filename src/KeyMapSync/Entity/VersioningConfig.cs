using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class VersioningConfig
{
    [Required]
    public Sequence Sequence { get; set; } = new();

    [Required]
    public SyncConfig SyncConfig { get; set; } = new();

    [Required]
    public VersionConfig VersionConfig { get; set; } = new();
}
