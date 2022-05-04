using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load;

public class LoadQuery
{
    public string Sql { get; set; }

    public ExpandoObject Parameter { get; set; }
}
