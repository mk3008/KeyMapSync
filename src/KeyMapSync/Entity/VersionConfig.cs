using KeyMapSync.DBMS;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class VersionConfig
{
    /// <summary>
    /// Version-table name format.
    /// </summary>
    public string TableNameFormat { get; set; } = "{0}__version";

    /// <summary>
    /// Datasourcename column name.
    /// </summary>
    public string DatasourceNameColumn { get; set; } = "datasource_name";

    public string BridgeInfoColumn { get; set; } = "bridge_info";

    /// <summary>
    /// Timestamp column name.
    /// </summary>
    public string TimestampColumn { get; set; } = "create_timestamp";

    private string GetTableName(Datasource d) => string.Format(TableNameFormat, d.Destination.DestinationTableName);

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
        tbl.AddDbColumn(BridgeInfoColumn, DbColumn.Types.Text);
        tbl.AddDbColumn(TimestampColumn, DbColumn.Types.Timestamp);

        return tbl;
    }

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
        p.AddColumnPair(":bridge_info", BridgeInfoColumn);

        return p;
    }

    public SqlCommand ToInsertCommand(IPier pier)
    {
        var d = pier.GetDatasource();

        var p = ToTablePair(pier);
        var ic = p.ToInsertCommand();
        ic.SelectSql.UseDistinct = true;
        var cmd = ic.ToSqlCommand();
        cmd.Parameters.Add(":datasource", d.DatasourceName);
        cmd.Parameters.Add(":bridge_info", Utf8Json.JsonSerializer.ToJsonString<object>(pier));
        return cmd;
    }
}
