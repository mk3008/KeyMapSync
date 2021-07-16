using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    /// <summary>
    /// Simple implementation of cascade datasource.
    ///
    /// Features
    /// - Not an extension.
    /// - Existence check is required.
    /// - Data source query is dynamic.(must override 'DatasourceQueryGenarator')
    /// </summary>
    public abstract class CascadeDatasourceMap : IDatasourceMap
    {
        public abstract string DestinationTableName { get; }

        public abstract string MappingName { get; }

        public abstract IEnumerable<string> DatasourceKeyColumns { get; }

        public virtual string DatasourceAliasName => "datasource";

        public abstract Func<SyncMap, string> DatasourceQueryGenarator { get; }

        public virtual Func<ExpandoObject> ParameterGenerator => null;

        public virtual bool IsNeedExistsCheck => true;

        public bool IsExtension => false;

        public IList<IDatasourceMap> Cascades { get; } = new List<IDatasourceMap>();

        public SyncMap Sender { get; set; }
    }
}