using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load
{
    public class TableDefinition : ILoadQueryable
    {
        public string TableName { get; set; }

        public string Sql { get; set; }

        public IList<ILoadQueryable> LoadQueryables => new List<ILoadQueryable>();

        public LoadQuery ToLoadQuery()
        {
            return new LoadQuery() { Sql = Sql };
        }
    }
}
