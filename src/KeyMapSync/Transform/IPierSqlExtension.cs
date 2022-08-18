using KeyMapSync.DBMS;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public static class IPierSqlExtension
{
    public static List<CteQuery> GetCteQueries(this IPier source)
    {
        var w = source.PreviousPrier?.GetCteQueries();
        if (w == null) w = new List<CteQuery>();

        var cte = new CteQuery()
        {
            AliasName = source.CteName,
            Query = source.ToSelectQuery()
        };

        w.Add(cte);

        return w;
    }

    public static SqlCommand ToCreateTableCommand(this IPier source)
    {
        var selectcmd = new SelectCommand()
        {
            CteQueries = source.GetCteQueries(),
        };

        var root = source.ToSelectTable();
        selectcmd.SelectTables.Add(root);

        //header
        source.ToHeaderSelectTable(root).ForEach(x => selectcmd.SelectTables.Add(x));

        //version
        var vconfig = source.GetDestination().VersioningConfig;
        if (vconfig != null)
        {
            var cte = new CteQuery()
            {
                AliasName = "_version",
                Query = source.ToSelectVersionSql()
            };
            selectcmd.CteQueries.Add(cte);

            var tbl = new SelectTable()
            {
                TableName = cte.AliasName,
                AliasName = "v",
                JoinType = JoinTypes.Cross,
            };
            tbl.AddSelectColumn(vconfig.Sequence.Column);

            selectcmd.SelectTables.Add(tbl);  
        }

        var prm = source.ToCreateTableParameter();
        if (prm != null) selectcmd.Parameters = prm;

        var sql = new CreateTableCommand(source.GetDatasource().BridgeName, selectcmd);
        sql.IsTemporary = true;
        var cmd = sql.ToSqlCommand();

        return cmd;
    }

    public static Dictionary<string, object>? ToCreateTableParameter(this IPier source)
    {
        var pier = source.CurrentPier;
        if (pier == null) return null;

        var current = pier.Filter?.ToParameters();
        var previous = pier.PreviousPrier?.ToCreateTableParameter();
        return current == null ? previous : current.Merge(previous);
    }

    private static string ToSelectVersionSql(this IPier source)
    {
        var config = source.GetDestination().VersioningConfig;
        if (config == null) throw new NotSupportedException($"versioning not supported.(table:{source.GetDestination().TableName})");
        var sql = $"select {config.Sequence.Command} as {config.Sequence.Column}";
        return sql;
    }

    private static List<SelectTable> ToHeaderSelectTable(this IPier source, SelectTable root)
    {
        var lst = new List<SelectTable>();
        var dest = source.GetDestination();
        var cnt = 0;

        foreach (var item in dest.Groups)
        {
            var cols = item.GetColumnsWithoutKey();
            var seq = item.Sequence.Column;

            var tbl = new SelectTable();

            tbl.TableName = $"(select head.*, {item.Sequence.Command} as {item.Sequence.Column}) from (select distinct {cols.ToString(", ")} from {source.ViewOrCteName}) head)";
            tbl.AliasName = $"g{cnt}";
            tbl.AddSelectColumns(item.GetColumnsWithoutKey());

            tbl.JoinFromTable = root;
            tbl.JoinType = JoinTypes.Left;
            tbl.AddJoinColumns(item.GetColumnsWithoutKey());

            lst.Add(tbl);

            cnt++;
        }

        return lst;
    }
}