using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class GroupDestination
{
    public int GroupDestinationId { get; set; }

    /// <summary>
    /// ex.integration_sale
    /// </summary>
    public string GroupDestinationName { get; set; } = string.Empty;

    /// <summary>
    /// 
    /// </summary>
    public List<string> GroupColumns { get; init; } = new ();

    public string SequenceKeyColumn { get; set; } = string.Empty;

    public string SequenceCommand { get; set; } = string.Empty;

    public string GetInnerAlias => $"__g{GroupDestinationId}";
}

