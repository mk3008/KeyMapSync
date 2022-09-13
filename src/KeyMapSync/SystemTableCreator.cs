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

public class SystemTableCreator
{
    public SystemTableCreator(IDbConnection connection, Datasource datasource, IDBMS dmbs)
    {
        Connection = connection;
        Datasource = datasource;
        Dbms = dmbs;
    }

    public Action<string>? Logger { get; set; } = null;

    public int? Timeout { get; set; } = null;

    private IDBMS Dbms { get; init; }

    private IDbConnection Connection { get; init; }

    private Datasource Datasource { get; init; }

    private Destination Destination => Datasource.Destination;

    private VersioningConfig? VersioningConfig => Destination.VersioningConfig;

    private OffsetConfig? OffsetConfig => Destination.KeyMapConfig?.OffsetConfig;

    public void Execute()
    {
        Execute(CreateKeyMapDbTable());
        Execute(CreateSyncDbTable());
        Execute(CreateVersionDbTable());
        Execute(CreateOffsetDbTable());

        foreach (var item in Datasource.Extensions)
        {
            var c = new SystemTableCreator(Connection, item, Dbms);
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

    private DbTable? CreateKeyMapDbTable()
    {
        var name = Datasource.GetKeymapTableName();
        if (name == null) return null;

        var t = new DbTable
        {
            Table = name,
            Sequence = null,
            Primarykeys = new() { Destination.Sequence.Column },
            UniqueKeyGroups = new() { Datasource.KeyColumns.Select(x => x.Key).ToList() }
        };

        t.AddDbColumn(Destination.Sequence.Column);
        Datasource.KeyColumns.ForEach(x => t.AddDbColumn(x.Key, x.Value));

        return t;
    }

    private DbTable? CreateSyncDbTable()
    {
        if (VersioningConfig == null) return null;
        var name = Datasource.GetSyncTableName();
        if (name == null) return null;

        var t = new DbTable
        {
            Table = name,
            Sequence = null,
            Primarykeys = new() { Destination.Sequence.Column },
        };

        t.AddDbColumn(Destination.Sequence.Column);
        t.AddDbColumn(VersioningConfig.Sequence.Column);

        return t;
    }

    private DbTable? CreateVersionDbTable()
    {
        if (VersioningConfig == null) return null;
        var name = Datasource.GetVersionTableName();
        if (name == null) return null;

        var t = new DbTable
        {
            Table = name,
            Sequence = VersioningConfig.Sequence,
            Primarykeys = new() { VersioningConfig.Sequence.Column },
        };

        t.AddDbColumn(VersioningConfig.Sequence.Column);
        t.AddDbColumn(VersioningConfig.VersionConfig.DatasourceNameColumn, DbColumn.Types.Text);
        t.AddDbColumn(VersioningConfig.VersionConfig.BridgeCommandColumn, DbColumn.Types.Text);
        t.AddDbColumn(VersioningConfig.VersionConfig.TimestampColumn, DbColumn.Types.Timestamp);

        return t;
    }

    private DbTable? CreateOffsetDbTable()
    {
        var tableName = Datasource.GetOffsetTableName();
        var offsetColumn = Datasource.GetOffsetColumnName();
        var renewColumn = Datasource.GetRenewalColumnName();
        if (OffsetConfig == null || tableName == null || offsetColumn == null || renewColumn == null) return null;

        var tbl = new DbTable
        {
            Table = tableName,
            Sequence = null,
            Primarykeys = new() { Destination.Sequence.Column },
            UniqueKeyGroups = new() { new() { offsetColumn }, new() { renewColumn } }
        };

        tbl.AddDbColumn(Destination.Sequence.Column);
        tbl.AddDbColumn(offsetColumn);
        tbl.AddDbColumn(renewColumn, isNullable: true);
        tbl.AddDbColumn(OffsetConfig.OffsetRemarksColumn, DbColumn.Types.Text);

        return tbl;
    }
}
