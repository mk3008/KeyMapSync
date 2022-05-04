using KeyMapSync.DBMS;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

/// <summary>
/// Manage transfer information.
/// </summary>
public class VersionConfig
{
    /// <summary>
    /// Table name format.
    /// </summary>
    [Required]
    public string TableNameFormat { get; set; } = "{0}__version";

    /// <summary>
    /// Datasourcename column name.
    /// </summary>
    [Required]
    public string DatasourceNameColumn { get; set; } = "datasource_name";

    /// <summary>
    /// Transfer settings.
    /// </summary>
    [Required]
    public string BridgeCommandColumn { get; set; } = "bridge_info";

    /// <summary>
    /// Timestamp column name.
    /// </summary>
    [Required]
    public string TimestampColumn { get; set; } = "create_timestamp";

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
            Sequence = versioning.Sequence,
            Primarykeys = new() { versioning.Sequence.Column },
        };

        tbl.AddDbColumn(versioning.Sequence.Column);
        tbl.AddDbColumn(DatasourceNameColumn, DbColumn.Types.Text);
        tbl.AddDbColumn(BridgeCommandColumn, DbColumn.Types.Text);
        tbl.AddDbColumn(TimestampColumn, DbColumn.Types.Timestamp);

        return tbl;
    }

    /// <summary>
    /// Convert to table and column mapping information.
    /// </summary>
    /// <param name="pier"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public TablePair ToTablePair(IPier pier)
    {
        var d = pier.GetDatasource();

        var config = d.Destination.VersioningConfig;
        if (config == null) throw new InvalidOperationException();

        var versionTable = ToDbTable(d, config);

        var p = new TablePair()
        {
            FromTable = d.BridgeName,
            ToTable = versionTable.Table
        };

        p.AddColumnPair(config.Sequence.Column);
        p.AddColumnPair(":datasource", DatasourceNameColumn);
        p.AddColumnPair(":bridge_command", BridgeCommandColumn);

        return p;
    }

    /// <summary>
    /// Convert to an additional query command.
    /// </summary>
    /// <param name="d"></param>
    /// <param name="sequencePrefix"></param>
    /// <returns></returns>
    public SqlCommand ToInsertCommand(IPier pier)
    {
        var d = pier.GetDatasource();

        var p = ToTablePair(pier);
        var ic = p.ToInsertCommand();
        ic.SelectSql.UseDistinct = true;
        var cmd = ic.ToSqlCommand();
        cmd.Parameters.Add(":datasource", d.DatasourceName);
        cmd.Parameters.Add(":bridge_command", Utf8Json.JsonSerializer.ToJsonString<object>(pier.Abutment.BridgeCommand));
        return cmd;
    }
}
