namespace KeyMapSync.DBMS;

public class SqlCommand
{
    public string CommandText { get; set; } = string.Empty;
    public Dictionary<string, object> Parameters { get; set; } = new();
}

