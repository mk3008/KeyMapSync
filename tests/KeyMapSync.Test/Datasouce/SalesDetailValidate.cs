using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Datasouce
{
    public class SalesDetailValidate : IValidateOption
    {
        public string Destination { get; } = "sales_detail";

        public IEnumerable<string> IgnoreColumns { get; } = new string[] { "sales_id", "sales_date" };

        public IEnumerable<string> ValueColumns { get; } = new string[] { "amount", "price" };
    }
}