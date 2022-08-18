using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class SelectTable
{
    public string TableName { get; set; } = string.Empty;

    public string AliasName { private get; set; } = string.Empty;

    public string GetAliasName() => (AliasName != string.Empty) ? AliasName : TableName;

    public List<SelectColumn> SelectColumns { get; set; } = new();

    public JoinTypes JoinType { get; set; } = JoinTypes.Root;

    public SelectTable? JoinFromTable { get; set; } = null;

    public Dictionary<string, string> JoinColumns { get; set; } = new();

    public void AddJoinColumn(string column) => JoinColumns.Add(column, column);

    public void AddJoinColumns(List<string> columns) => columns.ForEach(column => JoinColumns.Add(column, column));

    public void AddSelectColumn(string column) => SelectColumns.Add(new SelectColumn() { ColumnName = column });

    public void AddSelectColumn(SelectColumn column) => SelectColumns.Add(column);

    public void AddSelectColumns(List<string> columns) => columns.ForEach(column => SelectColumns.Add(new SelectColumn() { ColumnName = column }));

    public string ToFromSql()
    {
        var totable = (AliasName != string.Empty) ? $"{TableName} as {AliasName}" : TableName;
        var alias = GetAliasName();

        //var joincommand = JoinType.GetType().GetCustomAttributes(typeof(JoinCommandAttribute), false).First();
        var joincommand = GetFieldAttributes<JoinCommandAttribute, JoinTypes>(JoinType).First().Command;

        switch (JoinType)
        {
            case JoinTypes.Root:
                return totable;

            case JoinTypes.Inner:
            case JoinTypes.Left:
                if (JoinFromTable == null) throw new InvalidProgramException();
                var cnd = JoinColumns.Select((k, v) => $"{JoinFromTable.GetAliasName()}.{k} = {alias}.{v}").ToList();

                return $"{joincommand} {totable} on {cnd.ToString(" and ")}";

            case JoinTypes.Cross:
                return $"{joincommand} {totable}";

            default:
                throw new NotSupportedException();
        }
    }

    public List<string> ToColumnSqls() => SelectColumns.Select(x => x.ToSql(this)).ToList();

    public SelectCommand ToSelectCommand(string? whereText)
    {
        var cmd = new SelectCommand();
        cmd.SelectTables.Add(this);
        cmd.WhereText = whereText;
        return cmd;
    }

    private List<T> GetFieldAttributes<T, V>(V value) where T : Attribute where V : Enum
    {
        var lst = new List<T>();

        var tp = value.GetType();
        var name = tp.GetEnumName(value);
        if (name == null) return lst;

        var attrs = tp.GetField(name)?.GetCustomAttributes(typeof(T), false);
        if (attrs == null) return lst;

        foreach (var item in attrs) if (item is T atr) lst.Add(atr);

        return lst;
    }
}

public enum JoinTypes
{
    [JoinCommand]
    Root = 0,
    [JoinCommand(Command = "inner join")]
    Inner = 1,
    [JoinCommand(Command = "left join")]
    Left = 2,
    [JoinCommand(Command = "cross join")]
    Cross = 3,
}

public class JoinCommandAttribute : Attribute
{
    public string Command { get; set; } = string.Empty;
}