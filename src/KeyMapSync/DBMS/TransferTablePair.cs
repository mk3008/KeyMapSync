using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class TransferTablePair
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

    private SelectTable ToSelectTable()
    {
        var tbl = new SelectTable()
        {
            TableName = FromTable,
        };
        tbl.SelectColumns.AddRange(ColumnPairs.Select(x => new SelectColumn() { ColumnName = x.ToColumn, ColumnCommand = x.FromCloumn }));
        return tbl;
    }

    public InsertCommand ToInsertCommand()
    {
        var s = ToSelectTable().ToSelectCommand(Where);
        var cmd = new InsertCommand(ToTable, ColumnPairs.Select(x => x.ToColumn).ToList(), s);
        return cmd;
    }
}
