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

public class ExtensionAdditionalPier : PierBase
{
    public ExtensionAdditionalPier(IBridge bridge) : base(bridge) { }

    public override string CteName => "_insert";

    public override string AliasName => "d";

    public override string ToSelectQuery()
    {
        var where = Filter.ToCondition(this).ToWhereSqlText();
        var cmd = ToSelectTable().ToSelectCommand(where);

        return cmd.ToSqlCommand().CommandText;
    }

    public override SelectTable ToSelectTable()
    {
        var seq = this.GetDestination().Sequence;

        var tbl = new SelectTable()
        {
            TableName = this.Abutment.ViewName,
            AliasName = AliasName,
            JoinType = JoinTypes.Root,
        };

        tbl.SelectColumns.Add(new SelectColumn() { ColumnName = "*" });
        tbl.SelectColumns.Add(new SelectColumn() { ColumnName = seq.Column, ColumnCommand = seq.Command });

        return tbl;
    }
}

