using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace KeyMapSync.Database;

public partial class DbExecutor
{
    /// <summary>
    /// read table name info by table name.
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    private TableNameInfo ReadTableNameInfo(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var args = DB.GetFindTableNameInfoQuery(tableName);
        OnBeforeExecute?.Invoke(this, args);

        var lst = Connection.Query<TableNameInfo>(args.Sql, args.Param, commandTimeout: CommandTimeout).AsList();

        if (!lst.Any() == false) return null;
        if (lst.Count == 1) return lst.First();

        throw new ArgumentException("Multiple tables found.");
    }

    /// <summary>
    /// read sequence name info by table and column name.
    /// </summary>
    /// <param name="info"></param>
    /// <param name="sequenceColumn"></param>
    /// <returns></returns>
    public string ReadSequenceName(TableNameInfo info, string sequenceColumn)
    {
        var args = DB.GetSequenceNameScalar(Connection, info, sequenceColumn);
        OnBeforeExecute?.Invoke(this, args);

        return (string)Connection.ExecuteScalar(args.Sql, args.Param, commandTimeout: CommandTimeout);
    }

    /// <summary>
    /// read table columns
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public IEnumerable<string> ReadColumns(string tableName)
    {
        var table = ReadTable(tableName);
        return ReadColumns(table);
    }

    /// <summary>
    /// read table columns
    /// </summary>
    /// <param name="info"></param>
    /// <returns></returns>
    public IEnumerable<string> ReadColumns(TableNameInfo info)
    {
        var args = DB.GetColumnsSql(info);
        OnBeforeExecute?.Invoke(this, args);

        return Connection.Query<string>(args.Sql, args.Param, commandTimeout: CommandTimeout);
    }

    /// <summary>
    /// read sequence column
    /// </summary>
    /// <param name="info"></param>
    /// <returns></returns>
    public string ReadSequenceColumn(TableNameInfo info)
    {
        var args = DB.GetSequenceColumnScalar(info);
        OnBeforeExecute?.Invoke(this, args);
        return Connection.ExecuteScalar<string>(args.Sql, args.Param, commandTimeout: CommandTimeout);
    }

    /// <summary>
    /// create version table.
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="sequenceColumnName"></param>
    /// <param name="datasourceColumnName"></param>
    /// <param name="mappingColumnName"></param>
    public void CreateVersionTable(string tableName, string sequenceColumnName, string datasourceColumnName, string mappingColumnName)
    {
        var args = DB.GetCreateVersionTableDDL(tableName, sequenceColumnName, datasourceColumnName, mappingColumnName);
        OnBeforeExecute?.Invoke(this, args);
        Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
    }

    /// <summary>
    /// create sync table.
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="dest"></param>
    /// <param name="version"></param>
    public void CreateSyncTable(string tableName, Table dest, Table version)
    {
        var args = DB.GetCreateSyncTableDDL(tableName, dest, version);
        OnBeforeExecute?.Invoke(this, args);
        Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
    }

    /// <summary>
    /// create map table
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="dest"></param>
    /// <param name="datasourceKeyColumns"></param>
    public void CreateMappingTable(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns)
    {
        var args = DB.GetCreateKeymapTableDDL(tableName, dest, datasourceKeyColumns);
        OnBeforeExecute?.Invoke(this, args);
        Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
    }

    /// <summary>
    /// read table
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public Table ReadTable(string tableName)
    {
        var info = ReadTableNameInfo(tableName);
        if (info == null) return null;

        var table = new Table(info);
        var column = ReadSequenceColumn(info);

        if (column != null)
        {
            var name = ReadSequenceName(info, column);
            var command = DB.GetSequenceNextValueCommand(name);
            table.SequenceColumn = new SequenceColumn()
            {
                ColumnName = column,
                NextValCommand = command
            };
        }

        table.Columns = ReadColumns(info).ToList();
        return table;
    }

    /// <summary>
    /// read map table
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public Table ReadMappingTableInfo(string tableName)
    {
        var info = ReadTableNameInfo(tableName);
        if (info == null) return null;

        var table = new Table(info);
        table.Columns = ReadColumns(info).ToList();
        return table;
    }
}
