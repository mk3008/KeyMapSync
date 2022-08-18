using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class AdditionalPier : PierBase
{
    public AdditionalPier(IBridge bridge) : base(bridge)
    {
        Filter.Add(new NotExistsKeyMapCondition());
    }

    public override string CteName => "_added";

    public override string AliasName => "d";

    public override string ToSelectQuery()
    {
        var selectcmd = new SelectCommand();

        var root = ToSelectTable();
        selectcmd.SelectTables.Add(root);
        selectcmd.WhereText = Filter.ToCondition(this).ToWhereSqlText();

        return selectcmd.ToSqlCommand().CommandText;
    }

    public override SelectTable ToSelectTable()
    {
        var seq = this.GetDestination().Sequence;

        var tbl = new SelectTable()
        {
            TableName = Abutment.ViewName,
            AliasName = AliasName,
            JoinType = JoinTypes.Root,
        };

        tbl.AddSelectColumn("*");
        //tbl.AddSelectColumn(seq.ToSelectColumn());

        return tbl;
    }
}

