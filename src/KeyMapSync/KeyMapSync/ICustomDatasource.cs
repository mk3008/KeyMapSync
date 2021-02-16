using System.Collections.Generic;

namespace KeyMapSync
{
    public interface ICustomDatasource
    {
        /// <summary>
        /// mapping name
        /// </summary>
        public string MappingName { get; }

        /// <summary>
        /// datasource key column name list.
        /// </summary>
        public IEnumerable<string> DatasourceKeyColumns { get; }

        /// <summary>
        /// select datasource sql
        /// </summary>
        public string DatasourceQuery { get; }

        public string DatasourceAliasName => "datasource";

        /// <summary>
        /// generate datasource query parameter.<code>new() { id = 1}</code>
        /// </summary>
        /// <returns></returns>
        public object GetDatasourceQueryParameter() => null;
    }
}