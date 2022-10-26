using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class ExtendConfig
{
    /// <summary>
    /// KeyMa table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__ext";
}
