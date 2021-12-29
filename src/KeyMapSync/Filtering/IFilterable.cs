using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Filtering;

public interface IFilterable
{
    Filter ToFilter(IBridge sender);
}

