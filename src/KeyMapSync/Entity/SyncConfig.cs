using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class SyncConfig
{
    /// <summary>
    /// Sync-table name format.
    /// </summary>
    public string TableNameFormat { get; set; } = "{0}__sync";

    private string GetTableName(Datasource d) => string.Format(TableNameFormat, d.Destination.DestinationTableName);

    public DbTable ToDbTable(Datasource d, VersioningConfig versioning)
    {
        var name = GetTableName(d);

        var tbl = new DbTable
        {
            Table = name,
            Sequence = null,
            Primarykeys = new() { d.Destination.Sequence.Column },
        };
        tbl.AddDbColumn(d.Destination.Sequence.Column);
        tbl.AddDbColumn(versioning.Sequence.Column);

        return tbl;
    }

    public TablePair ToTablePair(Datasource d, string? sequencePrefix = null)
    {
        if (d.Destination.VersioningConfig == null) throw new InvalidOperationException();

        var syncTable = ToDbTable(d, d.Destination.VersioningConfig);

        var p = new TablePair()
        {
            FromTable = d.BridgeName,
            ToTable = syncTable.Table
        };

        var seq = d.Destination.Sequence;
        p.AddColumnPair($"{sequencePrefix}{seq.Column}", seq.Column);
        syncTable.GetColumnsWithoutKey().ForEach(x => p.AddColumnPair(x));

        return p;
    }

    public SqlCommand ToInsertCommand(Datasource d, string? sequencePrefix)
    {
        var p = ToTablePair(d, sequencePrefix);
        var cmd = p.ToInsertCommand().ToSqlCommand();

        return cmd;
    }
}
