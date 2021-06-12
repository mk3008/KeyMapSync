using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public abstract class RootDatasourceMap : IDatasourceMap
    {
        public abstract string DestinationTableName { get; }

        public abstract string MappingName { get; }

        public abstract IEnumerable<string> DatasourceKeyColumns { get; }

        public virtual string DatasourceAliasName => "datasource";

        public abstract string DatasourceQuery { get; }

        public Func<SyncMap, string> DatasourceQueryGenarator => (x) => DatasourceQuery;

        public virtual Func<object> ParameterGenerator => null;

        public virtual bool IsNeedExistsCheck => true;

        public bool IsExtension => false;

        public IList<IDatasourceMap> Cascades { get; } = new List<IDatasourceMap>();

        public SyncMap Sender { get; set; }
    }
}