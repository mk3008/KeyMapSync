using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    /// <summary>
    /// Manage data source queries and forwarding destinations.
    /// Key information is not managed, so get it from the <code>DatasourceTableName</code>.
    /// </summary>
    public interface ITableDatasourceMap
    {
        string DestinationTableName { get; }

        string DatasourceTableName { get; }

        string MappingName { get; }

        string DatasourceQuery { get; }

        string DatasourceAliasName { get; }

        abstract Func<ExpandoObject> ParameterGenerator { get; }

        IList<IDatasourceMap> Cascades { get; }
    }
}