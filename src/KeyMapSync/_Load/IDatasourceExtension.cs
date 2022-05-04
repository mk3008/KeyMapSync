using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load
{
    static class IDatasourceExtension
    {
        public static DatasourceAlias ToDatasourceMap(this IDatasource source)
        {
            var map = new DatasourceAlias()
            {
                AliasName = source.AliasName , 
                WithQueryText = source.WithQueryText, 
                ParameterSet = source.ParameterSet,
            };

            return map;
        }
    }
}
