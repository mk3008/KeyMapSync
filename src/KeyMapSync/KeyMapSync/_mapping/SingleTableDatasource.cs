using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    /// <summary>
    /// single table datasource
    /// </summary>
    public abstract class SingleTableDatasource : IDatasource
    {
        public virtual string DatasourceTableName { get; }

        public abstract string MappingName { get; }

        public IEnumerable<string> DatasourceKeyColumns => Enumerable.Empty<string>();

        public abstract string DatasourceQuery { get; }

        public string DatasourceAliasName => "datasource";

        public abstract Func<object> ParameterGenerator { get; }
    }
}