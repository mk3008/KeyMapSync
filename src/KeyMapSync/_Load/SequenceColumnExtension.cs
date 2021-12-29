using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load
{
    static class SequenceColumnExtension
    {
        public static SequenceMap ToSequenceMap(this SequenceColumn source)
        {
            var map = new SequenceMap ()
            {
                SequenceSqlText = source.NextValCommand, 
                ColumnAliasName = source.ColumnName,
            };

            return map;
        }
    }
}
