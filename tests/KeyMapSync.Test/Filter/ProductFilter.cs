using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Filter
{
    public class ProductFilter : DatasourceFilter
    {
        public ProductFilter(string name)
        {
            Name = name;
        }

        public string Name { get; set; }

        public override Func<IDatasourceMap, string> QueryGenerator => x => $@"
{x.DatasourceQueryGenarator.Invoke(null)}
,
{AliasName} as (
    select
        *
    from
        {x.DatasourceAliasName} d
    where
        d.product = :product
)
";

        public override Func<dynamic, ExpandoObject> ParameterGenerator => x =>
        {
            dynamic obj = (x != null) ? x : new ExpandoObject();
            obj.product = Name;
            return obj;
        };
    }
}