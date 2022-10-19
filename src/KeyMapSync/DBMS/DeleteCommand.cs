namespace KeyMapSync.DBMS;

public class DeleteCommand
{
    public string Table { get; init; } = string.Empty;

    public string WhereText { get; set; } = string.Empty;

    public SqlCommand ToSqlCommand()
    {
        if (WhereText == string.Empty) throw new NotSupportedException("Where Text is required.");

        var sql = $@"delete from {Table}
{WhereText}";

        return new SqlCommand() { CommandText = sql };
    }
}