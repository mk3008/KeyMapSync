using KeyMapSync.DBMS;
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
        tbl.AddDbColumn(TimestampColumn, DbColumn.Types.Timestamp);

        return tbl;
    }

    public TablePair ToTablePair(Datasource d)
    {
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

        return p;
    }

    public SqlCommand ToInsertCommand(Datasource d)
    {
        var p = ToTablePair(d);
        var ic = p.ToInsertCommand();
        ic.SelectSql.UseDistinct = true;
        var cmd = ic.ToSqlCommand();
        cmd.Parameters.Add(":datasource", d.DatasourceName);

        return cmd;
    }
}
