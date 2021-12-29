using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load
{
    public class ConditionQuery
    {
        public string ConditionSqlText { get; set; }

        public ExpandoObject Parameter { get; set; }

        public ConditionQuery Merge(ConditionQuery additional)
        {
            var query = new ConditionQuery() { ConditionSqlText = ConditionSqlText, Parameter = Parameter ?? new ExpandoObject() };

            if (additional == null) return query;

            if (string.IsNullOrEmpty(query.ConditionSqlText))
            {
                query.ConditionSqlText = additional.ConditionSqlText;
            }
            else if (!string.IsNullOrEmpty(additional.ConditionSqlText))
            {
                query.ConditionSqlText += $" and {additional.ConditionSqlText}";
            }
            query.Parameter.Merge(additional.Parameter);

            return query;
        }

        public string ToWhereSqlText()
        {
            return (!string.IsNullOrEmpty(ConditionSqlText) ? $" where {ConditionSqlText}" : "");
        }
    }
}
