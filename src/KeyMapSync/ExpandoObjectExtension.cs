using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    internal static class ExpandoObjectExtension
    {
        public static ExpandoObject Merge(this ExpandoObject source, ExpandoObject additional)
        {
            if (additional == null) return source;

            //Synthesize parameters
            var dic = source as IDictionary<string, object>;
            foreach (var item in dic)
            {
                if (dic.ContainsKey(item.Key))
                {
                    dic[item.Key] = item.Value;
                }
                else
                {
                    dic.Add(item.Key, item.Value);
                }
            }
            return source;
        }
    }
}
