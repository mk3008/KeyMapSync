namespace KeyMapSync.DBMS;
public class CreateTableCommand
{
    public CreateTableCommand(string table, SelectCommand sql)
    {
        Table = table;
        SelectSql = sql;
    }

    public string WithQuery { get; set; } = string.Empty;

    public string Table { get; init; }

    public bool IsTemporary { get; set; } = true;

    public SelectCommand SelectSql { get; init; }

    public SqlCommand ToSqlCommand()
    {

        var temporary = IsTemporary ? "temporary " : null;
        var with = WithQuery == string.Empty ? WithQuery : $"{WithQuery}\r\n";

        var cmd = SelectSql.ToSqlCommand();

        var sql = $@"create {temporary}table {Table}
as
{with}{cmd.CommandText};";

        return new SqlCommand() { CommandText = sql, Parameters = cmd.Parameters };
    }
}