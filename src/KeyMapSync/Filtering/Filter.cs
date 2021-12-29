using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Filtering;

public class Filter
{
    public string Condition { get; set; }

    public ExpandoObject Parameters { get; set; } 

    public IList<Filter> ToList()
    {
        var lst = new List<Filter>();
        lst.Add(this);
        return lst;
    }
}

