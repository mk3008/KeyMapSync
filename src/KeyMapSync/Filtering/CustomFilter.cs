using KeyMapSync.Filtering;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Filtering;

public class CustomFilter : IFilter
{
    public string Condition { get; set; }

    public Dictionary<string, object> Parameter { get; set; }

    public string ToCondition(IPier sender)
    {
        return string.Format(Condition, sender.GetInnerAlias());
    }

    public Dictionary<string, object> ToParameter()
    {
        return Parameter;
    }
}
