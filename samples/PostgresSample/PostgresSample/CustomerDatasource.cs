using KeyMapSync;
using System;
using System.Collections.Generic;

namespace PostgresSample
{
    internal class CustomerDatasource : SingleTableDatasource
    {
        public override string MappingName => "customer";

        public override string DatasourceTableName => "customer";

        public override string DatasourceQuery => @"
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

        public override Func<object> ParameterGenerator => () => new { name = "1" };
    }
}