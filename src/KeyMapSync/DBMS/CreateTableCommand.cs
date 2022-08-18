using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;
public class CreateTableCommand
{
    public CreateTableCommand(string table, SelectCommand sql)
    {
        Table = table;
        SelectSql = sql;
    }

    public string Table { get; init; }

    public bool IsTemporary { get; set; } = true;

    public SelectCommand SelectSql { get; init; }

    public SqlCommand ToSqlCommand()
    {
        var temporary = IsTemporary ? "temporary " : null;       
        var cmd = SelectSql.ToSqlCommand();

        var sql = $"create {temporary}table {Table}\r\nas\r\n{cmd.CommandText};";

        return new SqlCommand() { CommandText = sql, Parameters = cmd.Parameters };
    }
}