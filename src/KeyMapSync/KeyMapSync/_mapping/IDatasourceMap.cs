using System;
using System.Collections.Generic;
using System.Linq;

namespace KeyMapSync
{
    /// <summary>
    /// Manage data source queries and forwarding destinations.
    /// </summary>
    public interface IDatasourceMap
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
        string DatasourceQuery { get; }

        /// <summary>
        /// generate datasource query parameter.<code>new() { id = 1}</code>
        /// </summary>
        /// <returns></returns>
        Func<object> ParameterGenerator { get; }
    }
}