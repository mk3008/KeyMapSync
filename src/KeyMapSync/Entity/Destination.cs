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
/// It manages table names, columns, 
/// and sequence information of the transfer destination table. 
/// You can also specify how to manage the transfer.
/// </summary>
public class Destination
{
    /// <summary>
    /// The name of the transfer destination table.
    /// </summary>
    [Required]
    public string TableName { get; set; } = String.Empty;

    /// <summary>
    /// If the destination has a group header table, specify it.
    /// </summary>
    public List<GroupDestination> Groups { get; set; } = new();

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
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// Gets the columns of the transfer destination table excluding the sequence columns.
    /// </summary>
    /// <returns></returns>
    public List<string> GetInsertColumns() => Columns.Where(x => x != Sequence.Column).ToList();

    /// <summary>
    /// Specify if you want to reverse-lookup the source data source. 
    /// Also, specify if you want to perform differential transfer.
    /// Also, specify if you want to check the difference in the transferred data.
    /// </summary>
    public KeyMapConfig? KeyMapConfig { get; set; } = null;

    /// <summary>
    /// Specify this when you want to record the version number and transfer information for each transfer unit.
    /// </summary>
    public VersioningConfig? VersioningConfig { get; set; } = null;
}
