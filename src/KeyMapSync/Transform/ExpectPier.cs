using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

/// <summary>
/// TODO
/// </summary>
public class ExpectPier : PierBase
{
    public ExpectPier(IBridge bridge, IFilter? validateFilter = null) : base(bridge)
    {
        if (validateFilter != null) Filter.Add(validateFilter);
    }

    public override string CteName => "_expect";

    public override string AliasName => "d";

    public override string ToSelectQuery()
    {
        var selectcmd = new SelectCommand();

        var root = ToSelectTable();
        selectcmd.SelectTables.Add(root);
        selectcmd.SelectTables.Add(GetMapTable(root));
        selectcmd.SelectTables.AddRange(GetHeaderTables(root));

        return selectcmd.ToSqlCommand().CommandText;
    }

    public override SelectTable ToSelectTable()
    {
        var tbl = new SelectTable()
        {
            TableName = this.GetDatasource().Destination.TableName,
            AliasName = AliasName,
            JoinType = JoinTypes.Root,
        };

        tbl.SelectColumns.Add(new SelectColumn() { ColumnName = "*" });

        return tbl;
    }

    public List<SelectTable> GetHeaderTables(SelectTable fromTable)
    {
        var lst = new List<SelectTable>();
        var cnt = 0;

        foreach (var item in this.GetDatasource().Destination.Groups)
        {
            var tbl = new SelectTable()
            {
                TableName = item.TableName,
                AliasName = $"g{cnt}",
                JoinType = JoinTypes.Inner,
                JoinFromTable = fromTable,
            };
            tbl.JoinColumns.Add(item.Sequence.Column, item.Sequence.Column);
            tbl.SelectColumns.AddRange(item.GetColumnsWithoutKey().Select(x => new SelectColumn() { ColumnName = x }));

            lst.Add(tbl);
            cnt++;
        }

        return lst;
    }

    public SelectTable GetMapTable(SelectTable fromTable)
    {
        var ds = this.GetDatasource();
        var dest = this.GetDestination();
        var keymap = this.GetDestination().KeyMapConfig?.ToDbTable(ds);
        if (keymap == null) throw new InvalidOperationException();

        var tbl = new SelectTable()
        {
            TableName = keymap.Table,
            AliasName = "m",
            JoinType = JoinTypes.Inner,
            JoinFromTable = fromTable,
        };

        tbl.AddJoinColumn(dest.Sequence.Column);
        tbl.AddSelectColumns(ds.KeyColumns);

        return tbl;
    }
}

