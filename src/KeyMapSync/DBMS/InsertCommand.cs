namespace KeyMapSync.DBMS;

public class InsertCommand
{
    public InsertCommand(string table, List<string> columns, SelectCommand sql)
    {
        Table = table;
        columns.ForEach(x => Columns.Add(x));
        SelectSql = sql;
    }

    public string Table { get; init; }

    public List<string> Columns { get; init; } = new();

    public SelectCommand SelectSql { get; init; }

    public SqlCommand ToSqlCommand()
    {
        var column = Columns.ToString("\r\n, ").AddIndent(4);
        var cmd = SelectSql.ToSqlCommand();

        var sql = $@"insert into {Table} (
{column}
)
{cmd.CommandText};";

        return new SqlCommand() { CommandText = sql, Parameters = cmd.Parameters };
    }
}