using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform
{
    public class LowKeyMapFilterBuilder : ILoadKeyMapFilterBuilder
    {
        public IParameterSettable Build(IDatasource dataource, ILoad mapping)
        {
            var filter = new LowerIdKeyMap()
            {
                KeyColumns = dataource.DatasourceKeyColumns,
                TableName = $"{dataource.DestinaionTableName}_map_{dataource.MappingName}",
                Owner = mapping,
            };
            return filter;
        }
    }
}
