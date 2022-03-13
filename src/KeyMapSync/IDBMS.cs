using KeyMapSync.Entity;

namespace KeyMapSync;

public interface IDBMS {

    string ToCreateTableSql(DbTable tbl);
}

