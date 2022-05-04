using KeyMapSync.Validation;
using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

/// <summary>
/// Group header forwarding destination.
/// </summary>
public class GroupDestination
{
    /// <summary>
    /// The name of the transfer destination table.
    /// </summary>
    [Required]
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Sequence information of the transfer destination table.
    /// </summary>
    [Required]
    public Sequence Sequence { get; set; } = new();

    /// <summary>
    /// A group of columns in the destination table. 
    /// Please define including the sequence.
    /// </summary>
    [ListRequired]
    public List<string> Columns { get; init; } = new();

    /// <summary>
    /// Gets the alias name for SQL.
    /// </summary>
    /// <returns></returns>
    public string GetInnerAlias() => $"g_{TableName}".ToLower();

    /// <summary>
    /// Gets the columns of the transfer destination table excluding the sequence columns.
    /// </summary>
    /// <returns></returns>
    public List<string> GetColumnsWithoutKey() => Columns.Where(x => x != Sequence.Column).ToList();

    /// <summary>
    /// Convert to table and column mapping information for additional queries.
    /// </summary>
    /// <param name="d"></param>
    /// <returns></returns>
    public TablePair ToTablePair(Datasource d)
    {
        var pair = new TablePair()
        {
            FromTable = d.BridgeName,
            ToTable = TableName
        };

        pair.AddColumnPair(Sequence.Column);
        GetColumnsWithoutKey().Where(x => d.Columns.Contains(x)).ToList().ForEach(x => pair.AddColumnPair(x));

        return pair;
    }

    /// <summary>
    /// Convert to an additional query command.
    /// </summary>
    /// <param name="d"></param>
    /// <param name="sequencePrefix"></param>
    /// <returns></returns>
    public SqlCommand ToInsertCommand(Datasource d)
    {
        var p = ToTablePair(d);
        var ic = p.ToInsertCommand();
        ic.SelectSql.UseDistinct = true;
        var cmd = ic.ToSqlCommand();

        return cmd;
    }
}

