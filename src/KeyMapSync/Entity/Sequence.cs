using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class Sequence
{
    [Required]
    public string Column { get; set; } = String.Empty;

    [Required]
    public string Command { get; set; } = String.Empty;
}
