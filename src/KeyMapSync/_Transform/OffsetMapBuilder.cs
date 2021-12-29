using Dapper;
using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform
{
    public partial class OffsetMapBuilder : InsertLoadBuilder
    {
        public ILoadQueryable BuildAsOffset(IDatasource datasource)
        {
            if (datasource is Datasource) return BuildBridgeMappingAsOffset(datasource as Datasource);
            //if (datasource is HeaderDetailDatasource) return Build(datasource as HeaderDetailDatasource);

            throw new NotSupportedException();
        }
    }
}
