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

    public Dictionary<string, object> Parameters { get; set; } = new();

    public string? ConditionInfo => string.Format(Condition, "DATASOURCE");

    public string ToCondition(IPier sender) => string.Format(Condition, sender.AliasName);

    public Dictionary<string, object> ToParameters() => Parameters;
}
