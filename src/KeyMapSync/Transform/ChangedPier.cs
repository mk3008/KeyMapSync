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
public class ChangedPier : PierBase
{
    public ChangedPier(ExpectPier bridge) : base(bridge)
    {
        Filter.Add(DiffCondition);
    }

    public override string CteName => "_changed";

    public string TransformedAlias => "transed";

    public string CurrentAlias => "current";

    private DifferentCondition DiffCondition { get; set; } = new DifferentCondition();

    public override string AliasName => TransformedAlias;

    public override string ToSelectQuery()
    {
        var cmd = new SelectCommand();

        var root = ToSelectTable();
        var keymap = GetMapTable(root);
        var current = GetCurrentTable(root, keymap);

        cmd.SelectTables.Add(root);
        cmd.SelectTables.Add(keymap);
        cmd.SelectTables.Add(current);
        cmd.WhereText = Filter.ToCondition(this).ToWhereSqlText();

        return cmd.ToSqlCommand().CommandText;
    }

    public override SelectTable ToSelectTable()
    {
        var dest = this.GetDestination();

        //transformed destination data.
        var tbl = new SelectTable()
        {
            TableName = PreviousBridge.ViewOrCteName,
            AliasName = TransformedAlias,
            JoinType = JoinTypes.Root,
        };

        //origin key
        tbl.AddSelectColumn(dest.Sequence.Column);
        //origin header key
        tbl.AddSelectColumns(dest.Groups.Select(x => x.Sequence.Column).ToList());

        return tbl;
    }

    private SelectTable GetMapTable(SelectTable fromTable)
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

    private SelectTable GetCurrentTable(SelectTable transed, SelectTable keymapTable)
    {
        //current datasource data.
        var ds = this.GetDatasource();
        var dest = this.GetDestination();
        var config = dest.KeyMapConfig?.OffsetConfig;
        if (config == null) throw new InvalidProgramException();

        var current = new SelectTable()
        {
            TableName = Abutment.ViewName,
            AliasName = CurrentAlias,
            JoinType = JoinTypes.Left,
            JoinFromTable = keymapTable,
        };

        current.AddJoinColumns(ds.KeyColumns);

        var offsetkey = new SelectColumn()
        {
            ColumnName = $"{config.OffsetColumnPrefix}{dest.Sequence.Column}",
            ColumnCommand = $"{dest.Sequence.Command}"
        };

        var renewkey = new SelectColumn()
        {
            ColumnName = $"{config.RenewalColumnPrefix}{dest.Sequence.Column}",
            ColumnCommand = $"case when {current.GetAliasName()}.{ds.KeyColumns.First()} is null then null else {dest.Sequence.Command} end"
        };

        //offset key
        current.SelectColumns.Add(offsetkey);
        //offset remarks
        current.SelectColumns.Add(DiffCondition.ToRemarksColumn(this));

        //renewal key
        current.SelectColumns.Add(renewkey);
        //renewal values
        current.AddSelectColumn("*");

        return current;
    }
}

