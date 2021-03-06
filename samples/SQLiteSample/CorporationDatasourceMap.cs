﻿using KeyMapSync;
using System;
using System.Collections.Generic;
using System.Dynamic;

namespace SQLiteSample
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

        public Func<ExpandoObject> ParameterGenerator => () => null;

        public bool IsNeedExistsCheck => true;

        public bool IsExtension => false;

        public IList<IDatasourceMap> Cascades => new List<IDatasourceMap>();

        public Func<SyncMap, string> DatasourceQueryGenarator => (x) => DatasourceQuery;

        public bool IsBridge => false;

        public Type ActualDatasourceType => null;

        public bool IsOffset => false;

        public bool IsUpperCascade => false;
    }
}