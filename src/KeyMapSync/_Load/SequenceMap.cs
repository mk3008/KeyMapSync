using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load;

public class SequenceMap
{
    public SequenceColumn Column { get; set; }

    public string SequenceSqlText => Column.ColumnName;

    public string ColumnAliasName => Column.ColumnName;

    public string ColumnQueryText => $"{SequenceSqlText} as {ColumnAliasName}";
}

