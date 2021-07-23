using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace KeyMapSync
{
    public partial class DbExecutor
    {
        private TableNameInfo ReadTableNameInfo(string tableName)
        {
            var args = DB.GetFindTableNameInfoQuery(tableName);
            OnBeforeExecute?.Invoke(this, args);
            var lst = Connection.Query<TableNameInfo>(args.Sql, args.Param, commandTimeout: CommandTimeout).AsList();

            if (lst.Any() == false)
            {
                return null;
            }
            else if (lst.Count == 1)
            {
                return lst.First();
            }
            throw new ArgumentException("Multiple tables found.");
        }

        public string ReadSequenceName(TableNameInfo info, string sequenceColumn)
        {
            var args = DB.GetSequenceNameScalar(Connection, info, sequenceColumn);
            OnBeforeExecute?.Invoke(this, args);
            return (string)Connection.ExecuteScalar(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public IEnumerable<string> ReadColumns(TableNameInfo info)
        {
            var args = DB.GetColumnsSql(info);
            OnBeforeExecute?.Invoke(this, args);
            return Connection.Query<string>(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public string ReadSequenceColumn(TableNameInfo info)
        {
            var args = DB.GetSequenceColumnScalar(info);
            OnBeforeExecute?.Invoke(this, args);
            return Connection.ExecuteScalar<string>(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public void CreateSyncVersionTable(string tableName, string sequenceColumnName)
        {
            var args = DB.GetCreateSyncVersionTableDDL(tableName, sequenceColumnName);
            OnBeforeExecute?.Invoke(this, args);
            Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public void CreateSyncTable(string tableName, Table dest, Table version)
        {
            var args = DB.GetCreateSyncTableDDL(tableName, dest, version);
            OnBeforeExecute?.Invoke(this, args);
            Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

        public void CreateMappingTable(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns)
        {
            var args = DB.GetCreateMappingTableDDL(tableName, dest, datasourceKeyColumns);
            OnBeforeExecute?.Invoke(this, args);
            Connection.Execute(args.Sql, args.Param, commandTimeout: CommandTimeout);
        }

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

        public Table ReadMappingTableInfo(string tableName)
        {
            var info = ReadTableNameInfo(tableName);
            if (info == null) return null;

            var table = new Table(info);
            table.Columns = ReadColumns(info).ToList();
            return table;
        }
    }
}