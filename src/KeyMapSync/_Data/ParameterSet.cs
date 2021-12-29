using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Data;

/// <summary>
/// sql parameter set.
/// </summary>
public class ParameterSet
{
    public IDictionary<string, object> Parameters { get;} = new Dictionary<string, object>();

    /// <summary>
    /// condition sql text.
    /// ex. shop_id = :shop_id and sales_date = :sales_date
    /// </summary>
    public string ConditionSqlText { get; set; }

    public ParameterSet Merge(ParameterSet additional)
    {
        if (additional == null) return this;

        //deep copy
        var f = new ParameterSet() { ConditionSqlText = ConditionSqlText};
        Parameters.Action(x => f.Parameters.Add(x));

        //condition merge
        if (string.IsNullOrEmpty(f.ConditionSqlText))
        {
            f.ConditionSqlText = additional.ConditionSqlText;
        }
        else if (!string.IsNullOrEmpty(additional.ConditionSqlText))
        {
            f.ConditionSqlText += $" and {additional.ConditionSqlText}";
        }

        //parameter merge
        additional.Parameters.Where(x => !f.Parameters.Contains(x)).Action(x => f.Parameters.Add(x));

        return f;
    }

    public string ToWhereSqlText()
    {
        return (string.IsNullOrEmpty(ConditionSqlText) ? null : $" where {ConditionSqlText}");
    }

    public ExpandoObject ToExpandObject()
    {
        var exobj = new ExpandoObject();

        var dic = exobj as IDictionary<string, object>;
        Parameters.Action(x => dic.Add(x.Key, x.Value));

        return exobj;
    }
}
