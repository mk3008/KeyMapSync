using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync.Transform;

public static class IBridgeSqlExtension
{
    //    private static string CreateInsertSql(string toTable, IList<string> toColumns, string fromTable, IList<string>? fromColumns = null, bool useDistinct = false, string? withQuery = null, string? whereQuery = null)
    //    {
    //        fromColumns ??= toColumns;

    //        var toCols = toColumns.ToString(", ");
    //        var fromCols = fromColumns.ToString(", ");
    //        var distinct = useDistinct ? " distinct" : "";
    //        var with = (string.IsNullOrEmpty(withQuery)) ? null : $"\r\n{withQuery}";
    //        var where = (string.IsNullOrEmpty(whereQuery)) ? null : $"\r\n{whereQuery}";

    //        var sql = $@"insert into {toTable} (
    //    {toCols}
    //){with}
    //select{distinct}
    //    {fromCols}
    //from
    //    {fromTable}{where};";
    //        return sql;
    //    }

    //public static SqlCommand ToInsertDestinationCommand(this IBridge source, string? prefix)
    //{
    //    var dest = source.GetDestination();

    //    var toTable = dest.DestinationTableName;
    //    var fromTable = source.GetBridgeName();

    //    var info = dest.GetInsertDestinationInfo(prefix);

    //    var selectcmd = new SelectCommand()
    //    {
    //        Tables = { fromTable },
    //        Columns = info.fromCols,
    //        WhereText = info.where,
    //    };

    //    var sql = new InsertCommand(toTable, info.toCols, selectcmd);
    //    var cmd = sql.ToSqlCommand();

    //    return cmd;
    //}


    //public static SqlCommand ToInsertKeyMapCommand(this IBridge source, string? prefix)
    //{
    //    var ds = source.GetDatasource();

    //    var toTable = ds.KeyMapName;
    //    var fromTable = source.GetBridgeName();
    //    var info = ds.GetInsertKeyMapInfoset(prefix);

    //    var selectcmd = new SelectCommand()
    //    {
    //        Tables = { fromTable },
    //        Columns = info.fromColumns,
    //        WhereText = info.where,
    //    };

    //    var sql = new InsertCommand(toTable, info.toColumns, selectcmd);
    //    var cmd = sql.ToSqlCommand();

    //    return cmd;
    //}

    public static SqlCommand ToReverseInsertDestinationCommand(this IBridge source)
    {
        var d = source.GetDatasource();
        var config = d.Destination.KeyMapConfig?.OffsetConfig;
        if (config == null) throw new NotSupportedException();

        return config.ToReverseInsertDestinationCommand(d);
    }

    public static SqlCommand ToInsertOffsetCommand(this IBridge source)
    {
        var d = source.GetDatasource();
        var config = d.Destination.KeyMapConfig?.OffsetConfig;
        if (config == null) throw new NotSupportedException();

        return config.ToInsertCommand(d);
    }

    public static SqlCommand ToRemoveKeyMapCommand(this IBridge source)
    {
        var d = source.GetDatasource();
        var config = d.Destination.KeyMapConfig?.OffsetConfig;
        if (config == null) throw new NotSupportedException();

        return config.ToRmoveKeyMapCommand(d);
    }
}