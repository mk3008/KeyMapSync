using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class SelectCommand
{
    public List<CteQuery> CteQueries { get; set; } = new();

    public List<SelectTable> SelectTables { get; set; } = new();

    public bool UseDistinct { get; set; } = false;

    public string? WhereText { get; set; } = string.Empty;

    public Dictionary<string, object> Parameters { get; set; } = new();

    public void SetParameters(Dictionary<string, object>? prms)
    {
        if (prms == null) return;
        Parameters = prms;
    }

    public SqlCommand ToSqlCommand()
    {
        var cte = new StringBuilder();
        if (CteQueries.Any())
        {
            CteQueries.ForEach(x => cte.Append(((cte.Length == 0) ? "with\r\n" : ",\r\n") + x.ToSql()));
            cte.AppendLine();
        }

        var table = new StringBuilder();
        var column = new StringBuilder();
        if (SelectTables.Any())
        {
            SelectTables.ForEach(x => table.Append(((table.Length == 0) ? "\r\nfrom\r\n" : "\r\n") + x.ToFromSql()));
            table.AppendLine();

            var lst = new List<string>();
            SelectTables.ForEach(x => lst.AddRange(x.ToColumnSqls()));
            column.AppendLine();
            column.Append("    ");
            column.AppendLine(lst.ToString(", "));
            column.AppendLine();
        }

        var distinct = UseDistinct ? " distinct" : "";
        var where = WhereText == string.Empty ? WhereText : $"\r\n{WhereText}";

        var sql = $"{cte}select{distinct}{column}{table}{where}";

        return new SqlCommand() { CommandText = sql, Parameters = Parameters };
    }
}