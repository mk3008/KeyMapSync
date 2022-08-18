using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class SelectColumn
{
    public string ColumnName { get; set; } = string.Empty;

    public string ColumnCommand { get; set; } = string.Empty;

    public string ToSql(SelectTable tbl) => (ColumnCommand != string.Empty) ? $"{ColumnCommand} as {ColumnName}" : $"{tbl.GetAliasName()}.{ColumnName}";
}
