using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public abstract class BridgeBase : IBridge
{
    public abstract string Name { get; }

    public abstract IAbutment GetAbutment();

    public abstract IPier GetCurrentPier();

    protected string CreateInsertSql(string toTable, IList<string> toColumns, string fromTable, IList<string> fromColumns = null, bool useDistinct = false, string withQuery = null, string whereQuery = null)
    {
        fromColumns ??= toColumns;

        var toCols = toColumns.ToString(", ");
        var fromCols = fromColumns.ToString(", ");
        var distinct = useDistinct ? " distinct" : "";
        var with = (string.IsNullOrEmpty(withQuery)) ? null : $"\r\n{withQuery}";
        var where = (string.IsNullOrEmpty(whereQuery)) ? null : $"\r\n{whereQuery}";

        var sql = $@"insert into {toTable} (
    {toCols}
){with}
select{distinct}
    {fromCols}
from
    {fromTable}{where};";
        return sql;
    }

    public (string commandText, IDictionary<string, object> parameter) ToInsertDestinationCommand(string prefix)
    {
        var dest = this.GetDestination();

        var toTable = dest.DestinationName;
        var fromTable = this.GetBridgeName();

        var info = dest.GetInsertDestinationInfo(prefix);
        var sql = CreateInsertSql(toTable, info.toCols, fromTable, info.fromCols, whereQuery: info.where);
        return (sql, null);
    }

    public (string commandText, IDictionary<string, object> parameter) ToReverseInsertDestinationCommand()
    {
        var dest = this.GetDestination();
        var key = dest.SequenceKeyColumn;

        var toTable = dest.DestinationName;
        var fromTable = $"(select __p.offset_{key}, __d.* from {this.GetBridgeName()} __p inner join {dest.DestinationName} __d on __p.{key} = __d.{key}) __p";

        var info = dest.GetReverseInsertDestinationInfo();
        var sql = CreateInsertSql(toTable, info.toColumns, fromTable, info.fromColumns, whereQuery: info.where);
        return (sql, null);
    }


    public (string commandText, IDictionary<string, object> parameter) ToInsertKeyMapCommand(string prefix)
    {
        var ds = this.GetDatasource();

        var toTable = ds.KeyMapName;
        var fromTable = this.GetBridgeName();
        var info = ds.GetInsertKeyMapInfoset(prefix);

        var sql = CreateInsertSql(toTable, info.toColumns, fromTable, info.fromColumns, whereQuery: info.where);
        return (sql, null);
    }

    public (string commandText, IDictionary<string, object> parameter) ToInsertOffsetKeyMapCommand()
    {
        var dest = this.GetDatasource().Destination;

        var toTable = dest.OffsetName;
        var fromTable = this.GetBridgeName();
        var info = dest.GetInsertOffsetKeyMapInfoset();

        var sql = CreateInsertSql(toTable, info.toColumns, fromTable, info.fromColumns, whereQuery: info.where);
        return (sql, null);
    }

    public virtual string ToRemoveKeyMapSql() => null;

    public (string commandText, IDictionary<string, object> parameter) ToInsertSyncCommand(string prefix)
    {
        var dest = this.GetDestination();

        var toTable = dest.SyncName;
        var toCols = dest.GetSyncColumns();
        var fromTable = this.GetBridgeName();

        var info = dest.GetInsertSyncInfoset(prefix);
        var sql = CreateInsertSql(toTable, info.toCols, fromTable, info.fromCols, whereQuery: info.where);
        return (sql, null);
    }

    public (string commandText, IDictionary<string, object> parameter) ToInsertVersionCommand()
    {
        var cmd = (ToInsertVersionCommandText(), ToInsertVersionParameter());
        return cmd;
    }

    private string ToInsertVersionCommandText()
    {
        var dest = this.GetDestination();

        var toTable = dest.VersionName;
        var fromTable = this.GetBridgeName();

        var cols = new List<string>();
        cols.Add(dest.VersionKeyColumn);
        cols.Add(dest.NameColumn);

        var vals = new List<string>();
        vals.Add(dest.VersionKeyColumn);
        vals.Add(":name");

        var sql = CreateInsertSql(toTable, cols, fromTable, vals, useDistinct: true);
        return sql;
    }

    private IDictionary<string, object> ToInsertVersionParameter()
    {
        var ds = this.GetDatasource();
        var prm = new Dictionary<string, object>();
        prm[":name"] = ds.Name;
        return prm;
    }

    public IList<string> ToExtensionSqls()
    {
        var lst = new List<string>();

        var ds = this.GetDatasource();
        var bridgeName = this.GetBridgeName();

        foreach (var item in ds.Extensions)
        {
            var exDest = item.Destination;

            var dest = exDest.DestinationName;

            //create temporary view
            var view = CreateTemporaryViewDdl(item.Name, string.Format(item.QueryFormat, bridgeName));
            lst.Add(view.ddl);

            var cols = exDest.Columns;

            //create insert 
            var sql = CreateInsertSql(dest, cols, view.name);
            lst.Add(sql);
        }

        return lst;
    }

    private (string name, string ddl) CreateTemporaryViewDdl(string name, string query)
    {
        var viewName = $"{name}_{DateTime.Now.ToString("ssfff")}";
        var ddl = $@"create temporary view {viewName}
as
{query}";
        return (viewName, ddl);
    }
}

