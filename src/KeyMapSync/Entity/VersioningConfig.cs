using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class VersioningConfig
{
    public Sequence Sequence { get; set; } = new();

    public SyncConfig SyncConfig { get; set; } = new();

    public VersionConfig VersionConfig { get; set; } = new();
}
