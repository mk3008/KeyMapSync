using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public static class IPierExtension
{
    public static void AddFilter(this IPier source, IFilter f)
    {
        if (f == null) return;
        source.Filter.Add(f);
    }
}