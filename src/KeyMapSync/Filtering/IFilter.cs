using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Filtering;

public interface IFilter
{
    string ToCondition(IPier sender);

    string? ConditionInfo { get; }

    Dictionary<string, object>? Parameters { get; }
}

public class FilterContainer : IFilter
{
    public List<IFilter> Filters { get; set; } = new List<IFilter>();

    public Dictionary<string, object>? Parameters => ToParameters();

    public string? ConditionInfo => null;

    public void Add(IFilter item)
    {
        Filters.Add(item);
    }

    public string ToCondition(IPier sender)
    {
        return Filters.Select(x => x.ToCondition(sender)).ToString("\r\nand ");
    }

    public Dictionary<string, object>? ToParameters()
    {
        var prm = new Dictionary<string, object>();
        var hasValue = false;
        foreach (var item in Filters)
        {
            var p = item.Parameters;
            if (p != null)
            {
                hasValue = true;
                prm.Merge(p);
            }
        }
        return hasValue == false ? null : prm;
    }
}
