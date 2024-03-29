﻿using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    /// <summary>
    /// A class that just implements IDatasourceMap. For internal processing.
    /// </summary>
    internal class DatasourceMapWrap : IDatasourceMap
    {
        public DatasourceMapWrap()
        {
            DatasourceQueryGenarator = (x) => DatasourceQuery;
        }

        public string DestinationTableName { get; set; }

        public string MappingName { get; set; }

        public IEnumerable<string> DatasourceKeyColumns { get; set; }

        public string DatasourceAliasName { get; set; }

        public string DatasourceQuery { get; set; }

        public Func<ExpandoObject> ParameterGenerator { get; set; }

        public bool IsNeedExistsCheck { get; set; }

        public bool IsExtension { get; set; }

        public bool IsBridge { get; set; } = false;

        public IList<IDatasourceMap> Cascades { get; } = new List<IDatasourceMap>();

        public SyncMap Sender { get; set; }

        public Func<SyncMap, string> DatasourceQueryGenarator { get; set; }

        public Type ActualDatasourceType => null;

        public bool IsUpperCascade => false;

        public DatasourceFilter Filter { get; set; }
    }
}