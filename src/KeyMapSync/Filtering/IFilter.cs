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
    public string ToCondition(IBridge sender);

    public ExpandoObject ToParameter();
}

public class FilterContainer : IFilter
{
    public IList<IFilter> Filters { get; set; } = new List<IFilter>(); 

    public string ToCondition(IBridge sender)
    {
        return Filters.Select(x => x.ToCondition(sender)).ToString("\r\nand ");
    }

    public ExpandoObject ToParameter()
    {
        var prm = new ExpandoObject();
        var hasValue = false;
        foreach (var item in Filters)
        {
            var p = item.ToParameter();
            if (p != null)
            {
                hasValue = true;
                prm.Merge(p);
            }
        }
        return hasValue == false ? null : prm;
    }
}
