using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

/// <summary>
/// Manages key information conversion between transfer source and transfer destination. 
/// Used for difference transfer, 
/// difference check, 
/// and reverse lookup. 
/// Since the basic settings are specified by default, 
/// they can usually be used simply by declaring them.
/// </summary>
public class KeyMapConfig
{
    /// <summary>
    /// KeyMa table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__map_{1}";

    /// <summary>
    /// Difference check settings. 
    /// Specify if you want to perform difference transfer.
    /// </summary>
    public OffsetConfig? OffsetConfig { get; set; } = null;

    /// <summary>
    /// Get the table name.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    private string GetTableName(Datasource d) => string.Format(TableNameFormat, d.Destination.TableName, d.TableName);

    /// <summary>
    /// Get the DDL.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Convert to table and column mapping information.
    /// </summary>
    /// <param name="d"></param>
    /// <param name="sequencePrefix"></param>
    /// <returns></returns>
    public TransferTablePair ToTablePair(Datasource d, string? sequencePrefix = null)
    {
        var keymapDbTable = ToDbTable(d);

        var pair = new TransferTablePair()
        {
            FromTable = d.BridgeName,
            ToTable = keymapDbTable.Table
        };

        var seq = d.Destination.Sequence;
        pair.AddColumnPair($"{sequencePrefix}{seq.Column}", seq.Column);
        keymapDbTable.GetColumnsWithoutKey().Where(x => d.Columns.Contains(x)).ToList().ForEach(x => pair.AddColumnPair(x));

        return pair;
    }

    /// <summary>
    /// Convert to an additional query command.
    /// </summary>
    /// <param name="d"></param>
    /// <param name="sequencePrefix"></param>
    /// <returns></returns>
    public SqlCommand ToInsertCommand(Datasource d, string? sequencePrefix)
    {
        var p = ToTablePair(d, sequencePrefix);
        var cmd = p.ToInsertCommand().ToSqlCommand();

        return cmd;
    }

}
