using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal static class IDictionaryExtension
{
    public static Dictionary<string, object> Merge(this Dictionary<string, object> source, IDictionary<string, object>? additional)
    {
        if (additional != null) foreach (var item in additional) source[item.Key] = item.Value;
        return source;
    }
}