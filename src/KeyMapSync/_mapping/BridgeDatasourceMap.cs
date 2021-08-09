using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    public abstract class BridgeDatasourceMap : IDatasourceMap
    {
        public abstract string DestinationTableName { get; }

        public abstract string MappingName { get; }

        public abstract IEnumerable<string> DatasourceKeyColumns { get; }

        public virtual string DatasourceAliasName => "datasource";

        public abstract string DatasourceQuery { get; }

        public Func<SyncMap, string> DatasourceQueryGenarator => (x) => DatasourceQuery;

        public virtual Func<ExpandoObject> ParameterGenerator => null;

        public virtual bool IsNeedExistsCheck => true;

        public bool IsExtension => false;

        public bool IsBridge => true;

        public IList<IDatasourceMap> Cascades { get; } = new List<IDatasourceMap>();

        public SyncMap Sender { get; set; }

        public abstract Type ActualDatasourceType { get; }

        public bool IsUpperCascade => false;

        public virtual DatasourceFilter Filter => null;
    }
}