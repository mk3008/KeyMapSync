using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public abstract class DatasourceFilter
    {
        public virtual string AliasName => "filter_ds";

        public abstract Func<IDatasourceMap, string> QueryGenerator { get; }

        public abstract Func<dynamic, ExpandoObject> ParameterGenerator { get; }
    }
}