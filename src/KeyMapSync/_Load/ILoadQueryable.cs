using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load;

public interface ILoadQueryable
{
    LoadQuery ToLoadQuery();
}
