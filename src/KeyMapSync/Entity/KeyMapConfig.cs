using KeyMapSync.DMBS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class KeyMapConfig
{
    /// <summary>
    /// keymap table name format.
    /// </summary>
    public string TableNameFormat { get; set; } = "{0}__map_{1}";

    public OffsetConfig? OffsetConfig { get; set; } = null;

    private string GetTableName(Datasource d) => string.Format(TableNameFormat, d.Destination.DestinationTableName, d.DatasourceName);

    public DbTable ToDbTable(Datasource d)
    {
        var name = GetTableName(d);

        var tbl = new DbTable
        {
            Table = name,
            Sequence = null,
            Primarykeys = new() { d.Destination.Sequence.Column },
            UniqueKeyGroups = new() { d.KeyColumns }
        };

        tbl.AddDbColumn(d.Destination.Sequence.Column);
        d.KeyColumns.ForEach(x => tbl.AddDbColumn(x));

        return tbl;
    }

    public TablePair ToTablePair(Datasource d, string? sequencePrefix = null)
    {
        var keymapDbTable = ToDbTable(d);

        var pair = new TablePair()
        {
            FromTable = d.BridgeName,
            ToTable = keymapDbTable.Table
        };

        var seq = d.Destination.Sequence;
        pair.AddColumnPair($"{sequencePrefix}{seq.Column}", seq.Column);
        keymapDbTable.GetColumnsWithoutKey().Where(x => d.Columns.Contains(x)).ToList().ForEach(x => pair.AddColumnPair(x));

        return pair;
    }

    public SqlCommand ToInsertCommand(Datasource d, string? sequencePrefix)
    {
        var p = ToTablePair(d, sequencePrefix);
        var cmd = p.ToInsertCommand().ToSqlCommand();

        return cmd;
    }

}
