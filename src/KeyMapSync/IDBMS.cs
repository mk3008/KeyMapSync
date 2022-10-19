using KeyMapSync.DBMS;
using KeyMapSync.Entity;

namespace KeyMapSync;

public interface IDBMS
{
    string ToCreateTableSql(DbTable tbl);

    string GetSequenceSql();

    string GetColumnsSql();

    string GetKeyColumnsSql();

    //Destination ResolveDestination(string destinationname);

    //string ConcatFunctionToken { get; }

    //string ConcatSplitToken { get; }
}

