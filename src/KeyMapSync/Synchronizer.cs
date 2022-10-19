﻿using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using SqModel;
using System.Data;

namespace KeyMapSync;

public class Synchronizer
{
    public Synchronizer(SystemConfig config, IDBMS dbms)
    {
        SystemConfig = config;
        Dbms = dbms;
    }

    private SystemConfig SystemConfig { get; init; }

    private IDBMS Dbms { get; init; }

    public Action<string>? Logger { get; set; } = null;

    public void CreateTable(IDbConnection connection, Datasource datasource)
    {
        var exe = new SystemTableCreator(SystemConfig, Dbms, connection) { Logger = Logger };
        exe.Execute(datasource);
    }

    public Results Insert(IDbConnection connection, Datasource datasource, string argument = "", Action<SelectQuery>? act = null)
    {
        var exe = new InsertSynchronizer(SystemConfig, connection, datasource, act) { Logger = Logger, Argument = argument };
        return exe.Insert();
    }

    //public Results Offset(IDbConnection connection, Datasource datasource, Action<SelectQuery>? act = null)
    //{
    //    var exe = new OffsetSynchronizer(connection, datasource, Dbms, DestinationResolver, act) { Logger = Logger, Timeout = Timeout }; ;
    //    return exe.Offset();
    //}
}