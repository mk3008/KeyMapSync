using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static KeyMapSync.Entity.DbColumn;

namespace KeyMapSync.Entity;

public class DbTable
{
    public string Table { get; set; } = String.Empty;

    public Sequence? Sequence { get; set; }

    public List<string> Primarykeys { get; set; } = new();

    public List<List<string>> UniqueKeyGroups { get; set; } = new();

    public List<DbColumn> DbColumns { get; set; } = new();

    public List<string> GetColumnsWithoutKey() => DbColumns.Where(x => !Primarykeys.Contains(x.Column)).Select(x => x.Column).ToList();

    public void AddDbColumn(string columnName, Types type = DbColumn.Types.Numeric, bool isNullable = false)
    {
        DbColumns.Add(new DbColumn { Column = columnName, ColumnType = type, IsNullable = isNullable });
    }
}

public class DbColumn
{
    public string Column { get; set; } = string.Empty;

    public Types ColumnType { get; set; } = Types.Numeric;

    public bool IsNullable { get; set; } = false;

    public enum Types
    {
        Numeric = 0,
        Text = 1,
        Timestamp = 2
    }
}