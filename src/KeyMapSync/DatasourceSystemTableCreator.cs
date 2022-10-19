using Dapper;
using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal class DatasourceSystemTableCreator
{
    public DatasourceSystemTableCreator(IDbConnection connection, Datasource datasource, IDBMS dmbs, Func<string, Destination> resolver)
    {
        Connection = connection;
        Datasource = datasource;
        Dbms = dmbs;
        DestinationResolver = resolver;
    }

    public Action<string>? Logger { get; set; } = null;

    public int? Timeout { get; set; } = null;

    private IDBMS Dbms { get; init; }

    private IDbConnection Connection { get; init; }

    private Func<string, Destination> DestinationResolver { get; init; }

    private Datasource Datasource { get; init; }

    public void Execute()
    {
        Datasource.Destination = DestinationResolver(Datasource.DestinationName);

        Execute(Datasource.GetKeyMapDbTable());

        foreach (var item in Datasource.Extensions)
        {
            var c = new DatasourceSystemTableCreator(Connection, item, Dbms, DestinationResolver);
            c.Execute();
        }
    }

    private void Execute(DbTable? t)
    {
        if (t == null) return;
        var sql = Dbms.ToCreateTableSql(t);
        Logger?.Invoke(sql);

        Connection.Execute(sql, commandTimeout: Timeout);
    }
}
