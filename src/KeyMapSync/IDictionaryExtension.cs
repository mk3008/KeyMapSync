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

    public static Dictionary<string, string> Merge(this Dictionary<string, string> source, IDictionary<string, string>? additional)
    {
        if (additional != null) foreach (var item in additional) source[item.Key] = item.Value;
        return source;
    }

    public static void ForEach(this Dictionary<string, string> source, Action<string, string> action)
    {
        foreach (var item in source.Keys)
        {
            action(item, source[item]);
        }
    }
}