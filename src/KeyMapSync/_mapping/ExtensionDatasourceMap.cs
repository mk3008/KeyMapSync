using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    /// <summary>
    /// Simple implementation of extended datasource.
    ///
    /// Features
    /// - There is no point in managing mapping.
    /// - There is no need to check for existence.
    /// </summary>
    public abstract class ExtensionDatasourceMap : IDatasourceMap
    {
        public abstract string DestinationTableName { get; }

        public string MappingName => null;

        public IEnumerable<string> DatasourceKeyColumns => null;

        public virtual string DatasourceAliasName => "datasource";

        public abstract Func<SyncMap, string> DatasourceQueryGenarator { get; }

        public virtual Func<ExpandoObject> ParameterGenerator => null;

        public bool IsNeedExistsCheck => false;

        public bool IsExtension => true;

        public bool IsBridge => false;

        public IList<IDatasourceMap> Cascades { get; } = new List<IDatasourceMap>();

        public SyncMap Sender { get; set; }

        public Type ActualDatasourceType => null;

        public bool IsUpperCascade => false;
    }
}