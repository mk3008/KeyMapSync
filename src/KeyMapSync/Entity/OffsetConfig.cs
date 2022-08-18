using KeyMapSync.Validation;
using KeyMapSync.DBMS;
using KeyMapSync.Transform;
using System.ComponentModel.DataAnnotations;

namespace KeyMapSync.Entity;

/// <summary>
/// Difference transfer settings.
/// </summary>
public class OffsetConfig
{
    /// <summary>
    /// A group of columns whose signs need to be inverted when offsetting.
    /// </summary>
    /// <example>
    /// {"quantity", "price"}
    /// </example>
    [ListRequired]
    public List<string> SignInversionColumns { get; set; } = new();

    /// <summary>
    /// Table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__offset";

    /// <summary>
    /// Column prefix for offsetting.
    /// </summary>
    [Required]
    public string OffsetColumnPrefix { get; set; } = "offset_";

    /// <summary>
    /// Redesigned column prefix.
    /// </summary>
    [Required]
    public string RenewalColumnPrefix { get; set; } = "renewal_";

    /// <summary>
    /// A column that records the reasons for offsetting.
    /// </summary>
    [Required]
    public string OffsetRemarksColumn { get; set; } = "offset_remarks";

    private string GetOffsetTableName(Datasource d) => string.Format(TableNameFormat, d.Destination.TableName);

    private string GetOffsetColumnName(Datasource d) => $"{OffsetColumnPrefix}{d.Destination.Sequence.Column}";

    private string GetRenewalColumnName(Datasource d) => $"{RenewalColumnPrefix}{d.Destination.Sequence.Column}";

    /// <summary>
    /// Convert to DDL.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Convert to table and column mapping information.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    public TransferTablePair ToTablePair(Datasource d)
    {
        var offsetTable = ToDbTable(d);

        var pair = new TransferTablePair()
        {
            FromTable = d.BridgeName,
            ToTable = offsetTable.Table,
        };

        var seq = d.Destination.Sequence;
        pair.AddColumnPair($"{seq.Column}", seq.Column);
        offsetTable.GetColumnsWithoutKey().ForEach(x => pair.AddColumnPair(x));

        var where = offsetTable.GetColumnsWithoutKey().Select(x => $"{x} is not null").ToList().ToString(" and ");
        if (string.IsNullOrEmpty(where)) where = $"where {where}";

        return pair;
    }

    /// <summary>
    /// Convert to an additional query.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    public SqlCommand ToInsertCommand(Datasource d)
    {
        var p = ToTablePair(d);
        var cmd = p.ToInsertCommand().ToSqlCommand();
        return cmd;
    }

    /// <summary>
    /// Convert to a query that removes KeyMap.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public SqlCommand ToRmoveKeyMapCommand(Datasource d)
    {
        if (d.Destination.KeyMapConfig == null) throw new NotSupportedException($"destination is not support keymap.(table:{d.Destination.TableName})");

        var keymapTable = d.Destination.KeyMapConfig.ToDbTable(d).Table;
        var bridgeTable = d.BridgeName;
        var key = d.Destination.Sequence.Column;

        var cmd = new DeleteCommand()
        {
            Table = keymapTable,
            WhereText = $"where exists (select * from {bridgeTable} bridge where {keymapTable}.{key} = bridge.{key})"
        };

        return cmd.ToSqlCommand();
    }

    /// <summary>
    /// Convert to an additional query.(offset)
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    public SqlCommand ToInsertCommandForOffset(Datasource d)
    {
        var p = ToTablePairForOffset(d);
        var cmd = p.ToInsertCommand().ToSqlCommand();
        return cmd;
    }

    /// <summary>
    /// Convert to table and column mapping information.(offset)
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    public TransferTablePair ToTablePairForOffset(Datasource d)
    {
        var dest = d.Destination;
        var seq = dest.Sequence;

        var pair = new TransferTablePair()
        {
            FromTable = $"(select _new.offset_{seq.Column}, _old.* from {d.BridgeName} _new inner join {dest.TableName} _old on _new.{seq.Column} = _old.{seq.Column}) __p",
            ToTable = dest.TableName,
        };

        pair.AddColumnPair($"{OffsetColumnPrefix}{seq.Column}", seq.Column);
        dest.Groups.ForEach(x => pair.AddColumnPair(x.Sequence.Column));
        d.Columns.Where(x=> d.Destination.Columns.Contains(x)).ToList().ForEach(x =>
        {
            if (SignInversionColumns.Contains(x))
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
