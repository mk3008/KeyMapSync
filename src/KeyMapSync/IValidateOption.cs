using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public interface IValidateOption
    {
        IEnumerable<String> IgnoreColumns { get; }

        IEnumerable<String> PriceColumns { get; }
    }
}