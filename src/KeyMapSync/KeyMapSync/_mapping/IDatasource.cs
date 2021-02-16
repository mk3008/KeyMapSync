using System;
using System.Collections.Generic;
using System.Linq;

namespace KeyMapSync
{
    public interface IDatasource
    {
        /// <summary>
        /// mapping name
        /// </summary>
        public string MappingName { get; }

        /// <summary>
        /// datasource key column name list.
        /// </summary>
        public IEnumerable<string> DatasourceKeyColumns { get; }

        public string DatasourceAliasName { get; }

        /// <summary>
        /// select datasource sql
        /// </summary>
        public string DatasourceQuery { get; }

        /// <summary>
        /// generate datasource query parameter.<code>new() { id = 1}</code>
        /// </summary>
        /// <returns></returns>
        public Func<object> ParameterGenerator { get; }
    }
}