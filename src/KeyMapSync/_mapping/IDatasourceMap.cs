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

        /// <summary>
        /// Set to true if it is extended information.
        /// </summary>
        bool IsExtension { get; }

        /// <summary>
        /// If true, it will create a temporary table but will not transfer it to the destination.
        /// It is used when you want to perform secondary processing.
        /// /// </summary>
        bool IsBridge { get; }

        /// <summary>
        /// If it is a cascade to the upper layer (header table), set it to true.
        /// </summary>
        bool IsUpperCascade { get; }

        IList<IDatasourceMap> Cascades { get; }

        /// <summary>
        /// Specify the type of the actual data source map.
        /// Normally, null is fine.
        /// Specify this when transferring using a bridge data source.
        /// </summary>
        public Type ActualDatasourceType { get; }

        DatasourceFilter Filter { get; }
    }
}