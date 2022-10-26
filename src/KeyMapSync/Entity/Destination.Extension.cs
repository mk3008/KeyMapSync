using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public static class DestinationExtension
{
    public static string GetSyncTableName(this Destination source, SyncConfig config)
        => string.Format(config.TableNameFormat, source.TableName);

    public static string GetOffsetTableName(this Destination source, OffsetConfig config)
        => string.Format(config.TableNameFormat, source.TableName);

    public static string GetOffsetIdColumnName(this Destination source, OffsetConfig config)
        => $"{config.OffsetColumnPrefix}{source.SequenceConfig.Column}";

    public static string GetRenewalIdColumnName(this Destination source, OffsetConfig config)
        => $"{config.RenewalColumnPrefix}{source.SequenceConfig.Column}";

    public static DbTable GetSyncDbTable(this Destination source, SyncConfig config)
    {
        var name = source.GetSyncTableName(config);

        var t = new DbTable
        {
            Table = name,
            Sequence = null,
            Primarykeys = new() { source.SequenceConfig.Column },
        };

        t.AddDbColumn(source.SequenceConfig.Column);
        t.AddDbColumn("kms_process_id");

        return t;
    }

    public static DbTable GetOffsetDbTable(this Destination source, OffsetConfig config)
    {
        var tableName = source.GetOffsetTableName(config);
        var offsetColumn = source.GetOffsetIdColumnName(config);
        var renewColumn = source.GetRenewalIdColumnName(config);

        var tbl = new DbTable
        {
            Table = tableName,
            Sequence = null,
            Primarykeys = new() { source.SequenceConfig.Column },
            UniqueKeyGroups = new() { offsetColumn, renewColumn }
        };

        tbl.AddDbColumn(source.SequenceConfig.Column);
        tbl.AddDbColumn(offsetColumn);
        tbl.AddDbColumn(renewColumn, isNullable: true);
        tbl.AddDbColumn(config.OffsetRemarksColumn, DbColumn.Types.Text);

        return tbl;
    }
    public static string GetExtendTableName(this Destination source, ExtendConfig config)
    {
        return string.Format(config.TableNameFormat, source.TableName);
    }

    public static DbTable GetExtendDbTable(this Destination source, ExtendConfig config)
    {
        var name = source.GetExtendTableName(config);
        var seq = source.SequenceConfig;

        var lst = new Dictionary<string, DbColumn.Types>();
        lst.Add(seq.Column, DbColumn.Types.Numeric);
        lst.Add("extension_table_name", DbColumn.Types.Text);
        lst.Add("id", DbColumn.Types.Numeric);

        var t = new DbTable
        {
            Table = name,
            Sequence = null,
            Primarykeys = lst.Select(x => x.Key).ToList(),
        };

        lst.ForEach(x => t.AddDbColumn(x.Key, x.Value));

        return t;
    }
}
