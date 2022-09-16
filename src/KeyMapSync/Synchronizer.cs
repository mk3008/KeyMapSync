using KeyMapSync.Entity;
using SqModel;
using System.Data;

namespace KeyMapSync;

public class Synchronizer
{
    public Synchronizer(IDBMS dbms)
    {
        Dbms = dbms;
    }

    private IDBMS Dbms { get; init; }

    public Action<string>? Logger { get; set; } = null;

    public int? Timeout { get; set; } = null;

    public void CreateTable(IDbConnection connection, Datasource datasource)
    {
        var exe = new SystemTableCreator(connection, datasource, Dbms) { Logger = Logger, Timeout = Timeout };
        exe.Execute();
    }

    public Results Insert(IDbConnection connection, Datasource datasource, Action<SelectQuery>? act = null)
    {
        var exe = new InsertSynchronizer(connection, datasource, act) { Logger = Logger, Timeout = Timeout };
        return exe.Insert();
    }

    public Results Offset(IDbConnection connection, Datasource datasource, Action<SelectQuery>? act = null)
    {
        var exe = new OffsetSynchronizer(connection, datasource, Dbms, act) { Logger = Logger, Timeout = Timeout }; ;
        return exe.Offset();
    }
}