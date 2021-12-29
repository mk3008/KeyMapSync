using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Filtering;

public static class FilterExtension
{
    public static string ToWhereSqlText(this Filter source)
    {
        return string.IsNullOrEmpty(source.Condition) ? null : $"where {source.Condition}";
    }

    public static ExpandoObject ToExpandObject(this Filter source)
    {
        return source.Parameters;
    }

    public static string ToWhereSqlText(this IEnumerable<Filter> source)
    {
        var s = source.Where(x => !string.IsNullOrEmpty(x.Condition)).Select(x => x.Condition).ToString(" and ");
        if (!string.IsNullOrEmpty(s)) s = $"where {s}";
        return s;
    }

    public static ExpandoObject ToExpandObject(this IEnumerable<Filter> source)
    {
        var obj = new ExpandoObject();
        var dic = obj as IDictionary<string, object>;

        // merge parameter
        foreach (var kvs in source.Where(x => x.Parameters.Any()).Select(x => x.Parameters))
        {
            kvs.Where(x => !dic.ContainsKey(x.Key)).Action(x => dic.Add(x));
        }

        return obj;
    }
}

