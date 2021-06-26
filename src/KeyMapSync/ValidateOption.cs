using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public class ValidateOption : IValidateOption
    {
        public IList<String> IgnoreColumnList { get; } = new List<string>();

        public IList<String> PriceColumnList { get; } = new List<string>();

        public IEnumerable<string> IgnoreColumns => IgnoreColumnList;

        public IEnumerable<string> PriceColumns => PriceColumnList;
    }
}