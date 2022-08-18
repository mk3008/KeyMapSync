using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

/// <summary>
/// Database sequence.
/// </summary>
public class Sequence
{
    /// <summary>
    /// Column name.
    /// </summary>
    [Required]
    public string Column { get; set; } = String.Empty;

    /// <summary>
    /// SQL command to get the next sequence value.
    /// </summary>
    /// <example>
    /// > SQLite
    /// "(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sale' union all select 0)) + row_number() over()"
    /// </example>
    [Required]
    public string Command { get; set; } = String.Empty;

    public SelectColumn ToSelectColumn() => new SelectColumn() { ColumnName = Column, ColumnCommand = Command };
}
