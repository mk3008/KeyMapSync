using KeyMapSync.DMBS;
using KeyMapSync.Transform;

namespace KeyMapSync.Entity;

public class OffsetConfig
{
    /// <summary>
    /// ex
    /// "quantity, price"
    /// </summary>
    public List<string> SingInversionColumns { get; set; } = new();

    /// <summary>
    /// Offset-table name format.
    /// </summary>
    public string TableNameFormat { get; set; } = "{0}__offset";

    /// <summary>
    /// Offset column prefix.
    /// </summary>
    public string OffsetColumnPrefix { get; set; } = "offset_";

    /// <summary>
    /// Renewal column prefix.
    /// </summary>
    public string RenewalColumnPrefix { get; set; } = "renewal_";

    /// <summary>
    /// Offest remarks column
    /// </summary>
    public string OffsetRemarksColumn { get; set; } = "offset_remarks";

    private string GetOffsetTableName(Datasource d) => string.Format(TableNameFormat, d.Destination.DestinationTableName);

    private string GetOffsetColumnName(Datasource d) => $"{OffsetColumnPrefix}{d.Destination.Sequence.Column}";

    private string GetRenewalColumnName(Datasource d) => $"{RenewalColumnPrefix}{d.Destination.Sequence.Column}";

    public DbTable ToDbTable(Datasource d)
    {
        var tableName = GetOffsetTableName(d);
        var offsetColumn = GetOffsetColumnName(d);
        var renewColumn = GetRenewalColumnName(d);

        var tbl = new DbTable
        {
            Table = tableName,
            Sequence = null,
            Primarykeys = new() { d.Destination.Sequence.Column },
            UniqueKeyGroups = new() { new() { offsetColumn }, new() { renewColumn } }
        };

        tbl.AddDbColumn(d.Destination.Sequence.Column);
        tbl.AddDbColumn(offsetColumn);
        tbl.AddDbColumn(renewColumn, isNullable: true);
        tbl.AddDbColumn(OffsetRemarksColumn, DbColumn.Types.Text);

        return tbl;
    }

    public TablePair ToTablePair(Datasource d)
    {
        var offsetTable = ToDbTable(d);

        var pair = new TablePair()
        {
            FromTable = d.BridgeName,
            ToTable = offsetTable.Table,
        };

        var seq = d.Destination.Sequence;
        pair.AddColumnPair($"{seq.Column}", seq.Column);
        offsetTable.GetColumnsWithoutKey().Where(x => d.Columns.Contains(x)).ToList().ForEach(x => pair.AddColumnPair(x));

        var where = offsetTable.GetColumnsWithoutKey().Select(x => $"{x} is not null").ToList().ToString(" and ");
        if (string.IsNullOrEmpty(where)) where = $"where {where}";

        return pair;
    }

    public SqlCommand ToInsertCommand(Datasource d)
    {
        var p = ToTablePair(d);
        var cmd = p.ToInsertCommand().ToSqlCommand();
        return cmd;
    }

    public SqlCommand ToRmoveKeyMapCommand(Datasource d)
    {
        if (d.Destination.KeyMapConfig == null) throw new NotSupportedException($"destination is not support keymap.(table:{d.Destination.DestinationTableName})");

        var keymapTable = d.Destination.KeyMapConfig.ToDbTable(d).Table;
        var bridgeTable = d.BridgeName;
        var key = d.Destination.Sequence.Column;

        var sql = $@"delete from {keymapTable} keymap
where
    exists (select * from {bridgeTable} bridge where keymap.{key} = bridge.{key});";

        return new SqlCommand() { CommandText = sql };
    }

    public SqlCommand ToReverseInsertDestinationCommand(Datasource d)
    {
        var p = ToReverseTablePair(d);
        var cmd = p.ToInsertCommand().ToSqlCommand();
        return cmd;
    }

    public TablePair ToReverseTablePair(Datasource d)
    {
        var seq = d.Destination.Sequence;

        var pair = new TablePair()
        {
            FromTable = $"(select _new.offset_{seq.Column}, _old.* from {d.BridgeName} _new inner join {d.Destination.DestinationTableName} _old on _new.{seq.Column} = _old.{seq.Column}) __p",
            ToTable = d.Destination.DestinationTableName,
        };

        pair.AddColumnPair($"{OffsetColumnPrefix}{seq.Column}", seq.Column);
        d.Columns.ForEach(x =>
        {
            if (SingInversionColumns.Contains(x))
            {
                pair.AddColumnPair($"{x} * -1", x);
            }
            else
            {
                pair.AddColumnPair(x);
            }
        });

        return pair;
    }
}
