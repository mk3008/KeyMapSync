using KeyMapSync;
using System;
using System.Collections.Generic;

namespace KeyMapSync.Test
{
    internal class CorporationDatasourceMap : IDatasourceMap
    {
        public string DestinationTableName => "client";

        public string MappingName => "corporation";

        public IEnumerable<string> DatasourceKeyColumns => new string[] { "corporation_id" };

        public string DatasourceQuery => @"
with datasource as (
    select
        corporation_name as client_name
        , corporation_id
    from
        corporation
    order by
        corporation_id
)";

        public string DatasourceAliasName => "datasource";

        public Func<object> ParameterGenerator => () => null;
    }
}