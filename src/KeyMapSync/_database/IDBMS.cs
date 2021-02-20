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

        public SqlEventArgs GetCreateSyncVersionTableDDL(string tableName, string sequenceColumnName);

        public SqlEventArgs GetInsertVersionTableScalar(SyncMap def);

        public SqlEventArgs GetCreateSyncTableDDL(string tableName, Table dest, Table version);

        public SqlEventArgs GetCreateMappingTableDDL(string tableName, Table dest, IEnumerable<string> datasourceKeyColumns);
    }
}