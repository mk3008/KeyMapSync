using KeyMapSync.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load
{
    public class TableMap
    {
        /// <summary>
        /// ex."_bridge_999999"
        /// </summary>
        public string TableName { get; set; }

        public string AliasName { get; set; }

        public string TableQuery => $"{TableName} {AliasName}";
    }
}
