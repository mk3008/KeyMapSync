using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DMBS;

public class TablePair
{
    public string FromTable { get; set; } = string.Empty;

    public string ToTable { get; set; } = string.Empty;

    public List<ColumnPair> ColumnPairs { get; set; } = new();

    public string? Where { get; set; } = null;

    public void AddColumnPair(string fromColumn, string toColumn)
    {
        ColumnPairs.Add(new ColumnPair() { FromCloumn = fromColumn, ToColumn = toColumn });
    }
    public void AddColumnPair(string column)
    {
        ColumnPairs.Add(new ColumnPair() { FromCloumn = column, ToColumn = column });
    }

    public SelectCommand ToSelectCommand()
    {
        var cmd = new SelectCommand()
        {
            Tables = { FromTable },
            Columns = ColumnPairs.Select(x => x.FromCloumn).ToList(),
            WhereText = Where,
        };
        return cmd;
    }

    public InsertCommand ToInsertCommand()
    {
        var s = ToSelectCommand();
        var cmd = new InsertCommand(ToTable, ColumnPairs.Select(x => x.ToColumn).ToList(), s);
        return cmd;
    }
}
