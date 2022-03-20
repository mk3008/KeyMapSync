using KeyMapSync.Filtering;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace KeyMapSync.Filtering;

public class CustomFilter : IFilter
{
    public string Condition { get; set; } = String.Empty;

    public Dictionary<string, object> Parameter { get; set; } = new();

    public string Summary => GetSummary();

    private string GetSummary()
    {
        var s = string.Format(Condition + " ", "x");
        foreach (var item in Parameter)
        {
            var val = item.Value?.ToString();
            if (val == null)
            {
                s = Regex.Replace(s, $"{item.Key}\\s", "[NULL]");
            }
            else
            {
                s = Regex.Replace(s + " ", item.Key + "\\s", val);
            }
        }
        return $"{typeof(CustomFilter).Name} {s.Trim()}";
    }

    public string ToCondition(IPier sender) => string.Format(Condition, sender.GetInnerAlias());

    public Dictionary<string, object> ToParameter() => Parameter;
}
