using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public class DatasourceMap : IDatasourceMap
    {
        public string DestinationTableName { get; set; }

        public string MappingName { get; set; }

        public IEnumerable<string> DatasourceKeyColumns { get; set; }

        public string DatasourceQuery { get; set; }

        public string DatasourceAliasName { get; set; }

        public Func<object> ParameterGenerator { get; set; }
    }
}