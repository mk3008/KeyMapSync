using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Validation;

public static class Validator
{
    public static Dictionary<string, string> Execute(object obj, string prefix = "")
    {
        Dictionary<string, string> result = new();

        if (string.IsNullOrEmpty(prefix)) prefix = $"{obj.GetType().Name}.";

        foreach (var prop in obj.GetType().GetProperties())
        {
            var attributes = Validator.ReadAttributes<ValidationAttribute>(prop);
  
            var val = prop.GetValue(obj);
            attributes.ForEach(x =>
            {
                if (x is RequiredAttribute atr)
                {
                    if (!x.IsValid(val) || string.IsNullOrEmpty(val?.ToString())) result.Add($"{prefix}{prop.Name}", x.ErrorMessage ?? $"{prop.Name} is empry.");
                }
                else if (!x.IsValid(val)) result.Add($"{prefix}{prop.Name}", x.ErrorMessage ?? "undefined error.");
            });

            if (val is string || val is int || val == null) continue;

            if (val is IEnumerable lst)
            {
                foreach (var item in lst)
                {
                    if (item is string || item is int || item == null) continue;
                    var dic = Execute(item, $"{prefix}{prop.Name}.");
                    result = result.Merge(dic);
                }
            }
            else
            {
                var dic = Execute(val, $"{prefix}{prop.Name}.");
                result = result.Merge(dic);
            }
        }
        return result;
    }

    public static List<T> ReadAttributes<T>(PropertyInfo prop)
    {
        var lst = new List<T>();
        prop.GetCustomAttributes(typeof(T), true).ToList().ForEach(x =>
        {
            if (x is T atr) lst.Add(atr);
        });
        return lst;
    }
}

