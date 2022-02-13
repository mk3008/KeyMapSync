using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class SelectCommand
{
    public string WithQuery { get; set; } = string.Empty;

    public List<string> Columns { get; set; } = new();
    public List<string> Tables { get; set; } = new();

    public bool UseDistinct { get; set; } = false;
    public string? WhereText { get; set; } = string.Empty;

    public Dictionary<string, object> Parameters { get; set; } = new();

    public SqlCommand ToSqlCommand()
    {
        var with = (WithQuery == string.Empty) ? WithQuery : $"{WithQuery}\r\n";
        var distinct = UseDistinct ? " distinct" : "";
        var column = Columns.ToString("\r\n, ").AddIndent(4);
        var table = Tables.ToString("\r\n");
        var where = (WhereText == string.Empty) ?WhereText : $"\r\n{WhereText}";
        var sql = $@"{with}select{distinct}
{column}
from {table}{where}";

        return new SqlCommand() { CommandText = sql, Parameters = Parameters};
    }
}