using KeyMapSync;
using System;
using System.Collections.Generic;

namespace SQLiteSample
{
    internal class CustomerDatasourceMap : ITableDatasourceMap
    {
        public string DestinationTableName => "client";

        public string MappingName => "customer";

        public string DatasourceTableName => "customer";

        public string DatasourceQuery => @"
with datasource as (
    select
        customer_name as client_name
        , customer_id
        , 'test' as remarks
    from
        customer
    where
        customer_name like '%' || :name || '%'
    order by
        customer_id
)";

        public Func<object> ParameterGenerator => () => new { name = "1" };

        public string DatasourceAliasName => "datasource";

        public bool IsExtension => false;

        public IList<IDatasourceMap> Cascades => new List<IDatasourceMap>();
    }
}