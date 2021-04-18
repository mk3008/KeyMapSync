using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KeyMapSync
{
    /// <summary>
    /// string extension
    /// </summary>
    internal static class StringExtensions
    {
        /// <summary>
        /// string to teable info.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static TableNameInfo ToTableInfo(this string source)
        {
            var prms = source.Split('.');
            var info = new TableNameInfo();
            if (prms.Length == 1)
            {
                info.TableName = source;
            }
            else
            {
                info.SchemaName = prms[0];
                info.SchemaName = prms[1];
            }
            return info;
        }

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

        public static string Left(this string source, int length)
        {
            if (source.Length < length) return source;
            return source.Substring(0, length);
        }
    }
}