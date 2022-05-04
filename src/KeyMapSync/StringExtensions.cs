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
    public static string ToString(this IEnumerable<string> source, string splitter, Func<string, string>? func = null)
    {
        func ??= x => x;
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

    public static string AddIndent(this string source, int space, string separator = "\r\n")
    {
        var sp = "";
        for (int i = 0; i < space; i++) sp += " ";

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

    public static string RemoveOrDefault(this string source, int startIndex)
    {
        if (source.Length < startIndex) return source;
        return source.Remove(startIndex);
    }

    public static string? ToWhereSqlText(this string source)
    {
        return string.IsNullOrEmpty(source) ? null : $@"where
{source.AddIndent(4)}";
    }

    public static string ToWhereSqlText(this IEnumerable<string> source)
    {
        var s = source.Where(x => !string.IsNullOrEmpty(x)).Select(x => x).ToList().ToString(" and ");
        if (!string.IsNullOrEmpty(s)) s = $@"where
{s.AddIndent(4)}";
        return s;
    }
}
