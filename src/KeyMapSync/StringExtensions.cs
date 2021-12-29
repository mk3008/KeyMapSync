using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KeyMapSync;

/// <summary>
/// string extension
/// </summary>
internal static class StringExtensions
{
    public static string ToString(this IEnumerable<string> source, string splitter, Func<string, string> func = null)
    {
        if (func == null) func = (x => x);

        var s = new StringBuilder();
        var isFirst = true;
        foreach (var item in source)
        {
            if (!isFirst) s.Append(splitter);

            s.Append(func.Invoke(item));
            isFirst = false;
        }
        return s.ToString();
    }

    public static string Indent(this string source, int space, string separator = "\r\n")
    {
        var sp = "";
        for (int i = 0; i < space; i++)
        {
            sp += " ";
        }
        var sb = new StringBuilder();
        var isFirst = true;
        foreach (var item in source.Split(separator))
        {
            if (!isFirst) sb.Append(separator);
            sb.Append(sp).Append(item);
            isFirst = false;
        }
        return sb.ToString();
    }
}
