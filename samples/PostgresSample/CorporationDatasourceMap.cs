using KeyMapSync;
using System;
using System.Collections.Generic;
using System.Dynamic;

namespace PostgresSample
{
    internal class CorporationDatasourceMap : RootDatasourceMap
    {
        public override string DestinationTableName => "client";

        public override string MappingName => "corporation";

        public override IEnumerable<string> DatasourceKeyColumns => new string[] { "corporation_id" };

        public override string DatasourceQuery => @"
with datasource as (
    select
        corporation_name as client_name
        , corporation_id
    from
        corporation
    order by
        corporation_id
)";
    }
}