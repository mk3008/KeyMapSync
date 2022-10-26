using Dapper;
using KeyMapSync.Entity;
using System.Data;

namespace KeyMapSync.DBMS;

public class SystemTableCreator
{
    public SystemTableCreator(SystemConfig config, IDBMS dmbs, IDbConnection connection)
    {
        SystemConfig = config;
        Dbms = dmbs;
        Connection = connection;
    }

    public Action<string>? Logger { get; set; } = null;

    public SystemConfig SystemConfig { get; init; }

    public IDbConnection Connection { get; init; }

    private IDBMS Dbms { get; init; }

    public void Execute(Datasource datasource, bool isroot = true)
    {
        var destination = datasource.Destination;
        Execute(destination.GetSyncDbTable(SystemConfig.SyncConfig));

        if (destination.HeaderDestination != null)
        {
            Execute(destination.HeaderDestination.GetSyncDbTable(SystemConfig.SyncConfig));
        }

        if (isroot && !string.IsNullOrEmpty(datasource.MapName))
        {
            Execute(datasource.GetKeyMapDbTable(SystemConfig.KeyMapConfig));
            if (destination.AllowOffset) Execute(destination.GetOffsetDbTable(SystemConfig.OffsetConfig));
        }

        if (datasource.Extensions.Any())
        {
            Execute(datasource.Destination.GetExtendDbTable(SystemConfig.ExtendConfig));
        }

        datasource.Extensions.ForEach(x => Execute(x, false));
    }

    private void Execute(DbTable t)
    {
        if (t == null) return;
        var sql = Dbms.ToCreateTableSql(t);
        Logger?.Invoke(sql);
        Connection.Execute(sql);
    }
}
