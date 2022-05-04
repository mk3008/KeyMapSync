using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform
{
    public class NotExistsKeyMapFilterBuilder
    {
        public IParameterSettable Build(IDatasource dataource)
        {
            var filter = new UnSynchronizedFilter() {
                KeyColumns = dataource.DatasourceKeyColumns, 
                TableName = $"{dataource.DestinaionTableName}_map_{dataource.MappingName}" 
            };
            return filter;
        }
    }
}
