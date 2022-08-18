using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class CteQuery
{
    public string AliasName { get; set; } = string.Empty;

    public string Query { get; set; } = string.Empty;

    public string ToSql() => $"{AliasName} as (\r\n{Query.AddIndent(4)}\r\n)";
}
