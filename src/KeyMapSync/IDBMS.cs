using KeyMapSync.DBMS;

namespace KeyMapSync;

public interface IDBMS
{
    string ToCreateTableSql(DbTable tbl);

    string ConcatFunctionToken { get; }

    string ConcatSplitToken { get; }
}

