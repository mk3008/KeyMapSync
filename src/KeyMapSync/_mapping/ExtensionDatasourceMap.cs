using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public abstract class ExtensionDatasourceMap : IDatasourceMap
    {
        public abstract string DestinationTableName { get; }

        public string MappingName => null;

        public IEnumerable<string> DatasourceKeyColumns => null;

        public virtual string DatasourceAliasName => "datasource";

        public abstract Func<SyncMap, string> DatasourceQueryGenarator { get; }

        public virtual Func<object> ParameterGenerator => null;

        public bool IsNeedExistsCheck => false;

        public bool IsExtension => true;

        public IList<IDatasourceMap> Cascades { get; } = new List<IDatasourceMap>();

        public SyncMap Sender { get; set; }
    }
}