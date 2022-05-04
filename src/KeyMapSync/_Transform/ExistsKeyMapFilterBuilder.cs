using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform
{
    public class ExistsKeyMapFilterBuilder : ILoadKeyMapFilterBuilder
    {
        public string AliasName { get; set; } = "_keymap";

        public IParameterSettable Build(IDatasource dataource, ILoad mapping)
        {
            var filter = new ExistsKeyMapFilter() {
                DatasourceKeyNames = dataource.DatasourceKeyColumns, 
                TableName = $"{dataource.DestinaionTableName}_map_{dataource.MappingName}",
                Owner = mapping,
                AliasName = AliasName,
            };
            return filter;
        }
    }
}
