using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync
{
    /// <summary>
    /// Manage data source queries and forwarding destinations.
    /// </summary>
    public interface IDatasourceMap : IDatasourceMappable
    {
        /// <summary>
        /// destination table name
        /// </summary>
        string DestinationTableName { get; }

        /// <summary>
        /// mapping name
        /// </summary>
        string MappingName { get; }

        /// <summary>
        /// datasource key column name list.
        /// </summary>
        IEnumerable<string> DatasourceKeyColumns { get; }

        /// <summary>
        /// datasource query alias name
        /// </summary>
        string DatasourceAliasName { get; }

        /// <summary>
        /// select datasource sql
        /// </summary>
        Func<SyncMap, string> DatasourceQueryGenarator { get; }

        /// <summary>
        /// generate datasource query parameter.<code>new() { id = 1}</code>
        /// </summary>
        /// <returns></returns>
        Func<ExpandoObject> ParameterGenerator { get; }

        bool IsNeedExistsCheck { get; }

        bool IsExtension { get; }

        IList<IDatasourceMap> Cascades { get; }
    }
}