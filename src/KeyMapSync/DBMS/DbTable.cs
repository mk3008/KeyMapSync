using KeyMapSync.Entity;
using static KeyMapSync.DBMS.DbColumn;

namespace KeyMapSync.DBMS;

public class DbTable
{
    public string Table { get; set; } = string.Empty;

    public Sequence? Sequence { get; set; }

    public List<string> Primarykeys { get; set; } = new();

    public List<string> UniqueKeyGroups { get; set; } = new();

    public List<DbColumn> DbColumns { get; set; } = new();

    public List<string> GetInsertColumns() => DbColumns.Where(x => !Primarykeys.Contains(x.Column)).Select(x => x.Column).ToList();

    public void AddDbColumn(string columnName, DbColumnType type = DbColumnType.Numeric, bool isNullable = false)
    {
        DbColumns.Add(new DbColumn { Column = columnName, ColumnType = type, IsNullable = isNullable });
    }
}

public class DbColumn
{
    public string Column { get; set; } = string.Empty;

    public DbColumnType ColumnType { get; set; } = DbColumnType.Numeric;

    public bool IsNullable { get; set; } = false;
}

public enum DbColumnType
{
    Numeric = 0,
    Text = 1,
    Timestamp = 2,
    Date = 3,
    Bool = 4,

    NumericArray = 5,
}