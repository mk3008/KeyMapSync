using KeyMapSync.DBMS;

namespace KeyMapSync;

public interface IDBMS {

    string ToCreateTableSql(DbTable tbl);
}

