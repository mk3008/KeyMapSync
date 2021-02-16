using System.Collections.Generic;

namespace KeyMapSync
{
    public class Table : TableNameInfo
    {
        public Table(TableNameInfo nameInfo)
        {
            SchemaName = nameInfo.SchemaName;
            TableName = nameInfo.TableName;
        }

        public IEnumerable<string> Columns { get; set; }

        public SequenceColumn SequenceColumn { get; set; }
    }
}