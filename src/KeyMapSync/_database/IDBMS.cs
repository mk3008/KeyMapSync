using System.Collections.Generic;
using System.Data;

namespace KeyMapSync
{
    public interface IDBMS
    {
        SqlEventArgs GetFindTableNameInfoQuery(string name);

        SqlEventArgs GetFindTableNameInfoQuery(string schemaName, string tableName);

        SqlEventArgs GetColumnsSql(TableNameInfo info);

        public SqlEventArgs GetSequenceColumnScalar(TableNameInfo info);

        public SqlEventArgs GetSequenceNameScalar(IDbConnection cn, TableNameInfo info, string columnName);

        public string GetSequenceNextValueCommand(string sequenceName);

        public SqlEventArgs GetCreateVersionTableDDL(string tableName, string versionColumnName, string datasourceColumnName, string mappingColumnName);

        public SqlEventArgs GetInsertVersionTableScalar(SyncMap def);

        public SqlEventArgs GetCreateSyncTableDDL(string tableName, Table dest, Table version);

        public SqlEventArgs GetCreateKeymapTableDDL(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns);

        public SqlEventArgs GetCreateOffsetmapTableDDL(string tableName, Table dest, string offsetsourcePrefix, string offsetcomment);

        /// <summary>
        /// table name max length.
        /// </summary>
        public int TableNameMaxLength { get; }
    }
}