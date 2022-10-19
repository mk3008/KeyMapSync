namespace KeyMapSync;

internal static class DictionaryExtensions
{
    public static void ForEach<T1, T2>(this Dictionary<T1, T2> source, Action<KeyValuePair<T1, T2>> action) where T1 : notnull
    {
        foreach (var x in source) action(x);
    }
}
