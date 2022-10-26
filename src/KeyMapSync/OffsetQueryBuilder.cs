using KeyMapSync.Entity;
using SqModel.Analysis;
using SqModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

public static class OffsetQueryBuilder
{
    public static SelectQuery BuildSelectBridgeQuery(Datasource datasource, string argument, Action<SelectQuery, string>? injector = null)
    {
        /*
         * with
         * e as (
         *     --expectquery
         * ), 
         * select 
         *     offset_id
         *     , renewal_id
         *     , d.*
         *     , remarks
         * from e
         * left join datasource d on e.datasource_id = d.datasource_id
         * where 
         *     deleted or changed
         */
        var sq = BuildSelectExpectQuery(datasource, argument, injector);
        sq = BuildSelectBridgeQuery(sq);
        return sq;
    }

    private static SelectQuery BuildSelectExpectQuery(Datasource datasource, string argument, Action<SelectQuery, string>? injector = null)
    {
        /*
         * select d.*, m.datasource_id
         * from destination d
         * inner join keymap m on d.desitination_id = m.desitination_id
         * inner join sync s on m.desitination_id = s.destination_id
         * inner join version v on s.version_id = v.version_id
         * where 
         *     v.datasource_name = :dsname
         *     and injection
         */
        var destination = datasource.Destination;

        var alias = "d";

        var sq = new SelectQuery();
        var d = sq.From(destination.TableName).As(alias);
        sq.SelectAll(d);

        // relation
        sq = InjectKeymap(sq);

        //inject from custom function
        if (injector != null) injector(sq, argument);

        return sq;
    }

    private SelectQuery BuildSelectBridgeQuery(SelectQuery expectquery)
    {
        //TODO : InspectionIgnoreColumns

        /*
         * with
         * e as (
         *     --expectquery
         * ), 
         * select offset_id, renewal_id, d.*, remarks
         * from e
         * left join datasource d on e.datasource_id = d.datasource_id
         * where --deleted or changed
         */
        var expectalias = "e";
        var alias = "d";

        var sq = expectquery.PushToCommonTable(expectalias);
        var e = sq.From(expectalias);
        var d = e.LeftJoin(SqlParser.Parse(Datasource.Query)).As(alias).On(x =>
        {
            Datasource.KeyColumnsConfig.Select(x => x.Key).ToList().ForEach(col =>
            {
                x.Add().Equal(col);
            });
        });

        var cols = Destination.GetInsertColumns().ToList();

        var whereDeleted = (ConditionGroup g) =>
        {
            g.Add().Or().Column(d, Datasource.KeyColumnsConfig.First().Key).IsNull();
        };

        var whereChanged = (ConditionGroup g) =>
        {
            //value change
            //  epect.value <> d.value or not(expect.value is null and d.value is null)
            cols.ForEach(col =>
            {
                g.Add().Or().Column(e, col).NotEqual(d, col);
                g.AddGroup(g2 =>
                {
                    g2.Or().Not();
                    g2.Add().And().Column(e, col).IsNull();
                    g2.Add().And().Column(d, col).IsNull();
                });
            });
        };

        var selectOffsetId = (SelectItem item) =>
        {
            item.CaseWhen(w =>
            {
                w.Add().WhenGroup(g =>
                {
                    whereDeleted(g);
                    whereChanged(g);
                }).Then(Destination.SequenceConfig.Command);
                w.Add().ElseNull();
            }).As(OffsetConfig.OffsetIdColumn);
        };

        var selectRenewalId = (SelectItem item) =>
        {
            item.CaseWhen(w =>
            {
                w.Add().WhenGroup(g =>
                {
                    whereChanged(g);
                }).Then(Destination.Sequence.Command);
                w.Add().ElseNull();
            }).As(RenewalIdColumn);
        };

        var selectRemarks = (SelectItem item) =>
        {
            item.Concat(Dbms.ConcatFunctionToken, Dbms.ConcatSplitToken, lst =>
            {
                //case when datasource_id is null then 'deleted' end
                lst.Add().CaseWhen(w =>
                {
                    w.Add().When(x => x.Column(e, Datasource.KeyColumns.First().Key).IsNull()).Then("'deleted'");
                });

                //case when a.val <> b.val or not(a.val is null and b.val is null) then 'val is changed' end
                cols.ForEach(col =>
                {
                    lst.Add().CaseWhen(w =>
                    {
                        w.Add().WhenGroup(g =>
                        {
                            g.Add().Or().Column(e, col).NotEqual(d, col);
                            g.AddGroup(g2 =>
                            {
                                g2.Or().Not();
                                g2.Add().And().Column(e, col).IsNull();
                                g2.Add().And().Column(d, col).IsNull();
                            });
                        }).Then($"'{col} is changed,'");
                    });
                });
            }).As(OffsetRemarksColumn);
        };

        //select
        InjectSelectVersion(sq);
        sq.Select(e, Destination.Sequence.Column);
        selectOffsetId(sq.Select.Add());
        selectRenewalId(sq.Select.Add());
        sq.SelectAll(d);
        selectRemarks(sq.Select.Add());

        //where
        sq.Where.Add().CaseWhen(w =>
        {
            w.Add().WhenGroup(g =>
            {
                whereDeleted(g);
                whereChanged(g);
            }).Then(true);
        });

        return sq;
    }

    private static SelectQuery InjectKeymap(SelectQuery sq)
    {
        /* 
         * select d.*, m.datasource_id
         * from destination d
         * inner join keymap m on d.desitination_id = _m.desitination_id
         * inner join sync s on _m.desitination_id = _s.destination_id
         * inner join version v on _s.version_id = _v.version_id
         * where
         * v.datasource_name = :dsname
         */
        var keymapalias = "m";
        var syncalias = "s";
        var veralias = "v";

        //relation
        var keymap = Datasource.GetKeymapTableName();
        if (keymap == null) throw new InvalidProgramException();
        var m = sq.FromClause.InnerJoin(keymap).As(keymapalias).On(Destination.Sequence.Column);

        var sync = Datasource.GetSyncTableName();
        if (sync == null) throw new InvalidProgramException();
        var s = m.InnerJoin(sync).As(syncalias).On(Destination.Sequence.Column);

        var ver = Datasource.GetVersionTableName();
        if (ver == null) throw new InvalidProgramException();
        var v = s.InnerJoin(ver).As(veralias).On(VersioningConfig.Sequence.Column);

        //select
        Datasource.KeyColumns.ForEach(x => sq.Select(m, x.Key));

        //where
        sq.Where.Add().Column(v, VersionConfig.DatasourceNameColumn).Equal(":dsname").Parameter(":dsname", Datasource.TableName);

        return sq;
    }
}
