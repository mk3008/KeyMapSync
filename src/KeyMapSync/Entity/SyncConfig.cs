using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

/// <summary>
/// Manage the conversion of transferred data and version data.
/// </summary>
public class SyncConfig
{
    /// <summary>
    /// Table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__sync";

    /// <summary>
    /// Get the table name.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    private string GetTableName(Datasource d) => string.Format(TableNameFormat, d.Destination.TableName);

    /// <summary>
    /// Get the DDL.
    /// </summary>
    /// <param name="d"></param>
    /// <param name="versioning"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Convert to table and column mapping information.
    /// </summary>
    /// <param name="d"></param>
    /// <param name="sequencePrefix"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
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
