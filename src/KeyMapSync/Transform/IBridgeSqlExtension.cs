using KeyMapSync.Entity;
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

    public static SqlCommand ToInsertDestinationCommand(this IBridge source, string? prefix)
    {
        var dest = source.GetDestination();

        var toTable = dest.DestinationName;
        var fromTable = source.GetBridgeName();

        var info = dest.GetInsertDestinationInfo(prefix);

        var selectcmd = new SelectCommand()
        {
            Tables = { fromTable },
            Columns = info.fromCols,
            WhereText = info.where,
        };

        var sql = new InsertCommand(toTable, info.toCols, selectcmd);
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static SqlCommand ToReverseInsertDestinationCommand(this IBridge source)
    {
        var dest = source.GetDestination();
        var key = dest.SequenceKeyColumn;

        var toTable = dest.DestinationName;
        var fromTable = $"(select __p.offset_{key}, __d.* from {source.GetBridgeName()} __p inner join {dest.DestinationName} __d on __p.{key} = __d.{key}) __p";

        var info = dest.GetReverseInsertDestinationInfo();

        var selectcmd = new SelectCommand()
        {
            Tables = { fromTable },
            Columns = info.fromColumns,
            WhereText = info.where,
        };

        var sql = new InsertCommand(toTable, info.toColumns, selectcmd);
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static SqlCommand ToInsertKeyMapCommand(this IBridge source, string? prefix)
    {
        var ds = source.GetDatasource();

        var toTable = ds.KeyMapName;
        var fromTable = source.GetBridgeName();
        var info = ds.GetInsertKeyMapInfoset(prefix);

        var selectcmd = new SelectCommand()
        {
            Tables = { fromTable },
            Columns = info.fromColumns,
            WhereText = info.where,
        };

        var sql = new InsertCommand(toTable, info.toColumns, selectcmd);
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static SqlCommand ToInsertOffsetKeyMapCommand(this IBridge source)
    {
        var dest = source.GetDatasource().Destination;

        var toTable = dest.OffsetName;
        var fromTable = source.GetBridgeName();
        var info = dest.GetInsertOffsetKeyMapInfoset();

        var selectcmd = new SelectCommand()
        {
            Tables = { fromTable },
            Columns = info.fromColumns,
            WhereText = info.where,
        };

        var sql = new InsertCommand(toTable, info.toColumns, selectcmd);
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static string ToRemoveKeyMapSql(this IBridge source)
    {
        var ds = source.GetDatasource();
        var dest = source.GetDestination();

        var toTable = ds.KeyMapName;
        var fromTable = source.GetBridgeName();
        var key = dest.SequenceKeyColumn;

        var sql = $@"delete from {toTable}
where
    exists (select * from {fromTable} t where {toTable}.{key} = t.{key});";
        return sql;
    }

    public static SqlCommand ToInsertSyncCommand(this IBridge source, string? prefix)
    {
        var dest = source.GetDestination();

        var toTable = dest.SyncName;
        var toCols = dest.GetSyncColumns();
        var fromTable = source.GetBridgeName();

        var info = dest.GetInsertSyncInfoset(prefix);

        var selectcmd = new SelectCommand()
        {
            Tables = { fromTable },
            Columns = info.fromCols,
            WhereText = info.where,
        };

        var sql = new InsertCommand(toTable, info.toCols, selectcmd);
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static SqlCommand ToInsertVersionCommand(this IBridge source)
    {
        var dest = source.GetDestination();

        var toTable = dest.VersionName;
        var fromTable = source.GetBridgeName();

        var cols = new List<string>();
        cols.Add(dest.VersionKeyColumn);
        cols.Add(dest.NameColumn);

        var vals = new List<string>();
        vals.Add(dest.VersionKeyColumn);
        vals.Add(":name");

        var ds = source.GetDatasource();
        var prm = new Dictionary<string, object>();
        prm[":name"] = ds.Name;

        var selectcmd = new SelectCommand()
        {
            Tables = { fromTable },
            Columns = vals,
            UseDistinct = true,
            Parameters = prm
        };

        var sql = new InsertCommand(toTable, cols, selectcmd);
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static IList<string> ToExtensionSqls(this IBridge source)
    {
        var lst = new List<string>();

        var ds = source.GetDatasource();
        var bridgeName = source.GetBridgeName();

        foreach (var item in ds.Extensions)
        {
            var exDest = item.Destination;
            if (exDest == null) throw new NullReferenceException(nameof(exDest));

            var dest = exDest.DestinationName;

            //create temporary view
            var view = CreateTemporaryViewDdl(item.Name, string.Format(item.QueryFormat, bridgeName));
            lst.Add(view.ddl);

            //create insert 
            var cols = exDest.Columns;

            var selectcmd = new SelectCommand()
            {
                Tables = { view.name },
                Columns = cols
            };

            var sql = new InsertCommand(dest, cols, selectcmd);
            var cmd = sql.ToSqlCommand();

            lst.Add(cmd.CommandText);
        }

        return lst;
    }

    public static SqlCommand ToInsertHeaderCommand(this IBridge source, GroupDestination grp)
    {
        var toTable = grp.GroupDestinationName;
        var fromTable = source.GetBridgeName();

        var cols = new[] { grp.SequenceKeyColumn }.Union(grp.GroupColumns).ToList();

        var selectcmd = new SelectCommand()
        {
            Tables = { fromTable },
            Columns = cols,
            UseDistinct = true
        };

        var sql = new InsertCommand(toTable, cols, selectcmd);
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    private static (string name, string ddl) CreateTemporaryViewDdl(string name, string query)
    {
        var viewName = $"{name}_{DateTime.Now.ToString("ssfff")}";
        var ddl = $@"create temporary view {viewName}
as
{query}";
        return (viewName, ddl);
    }
}