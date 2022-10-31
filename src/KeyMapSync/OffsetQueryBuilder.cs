﻿using KeyMapSync.Entity;
using SqModel.Analysis;
using SqModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqModel.Expression;

namespace KeyMapSync;

internal class OffsetQueryBuilder
{
    internal static Query BuildSelectOffsetDatasourceFromBridge(Datasource ds, string bridgeName, SystemConfig config)
    {
        /*
         * select bridge.offset_id as destination_id, e.value1, e.value2 * -1 as value2
         * from bridge
         * inner join destination expect on bridge.destination_id = expect.destination_id
         * inner join extension1 expect_ext1 on expect.? = expect_ext1.?
         */

        //from
        var sq = new SelectQuery();
        var bridge = sq.From(bridgeName).As("bridge");
        var e = bridge.InnerJoin(ds.Destination.TableName).As("e").On(ds.Destination.SequenceConfig.Column);

        var addSelectColumn = () =>
        {
            var dscols = SqlParser.Parse(ds.Query).Select.GetColumnNames();
            var cols = ds.Destination.GetInsertColumns().Where(x => dscols.Contains(x)).ToList();
            cols.ForEach(x =>
            {
                if (ds.Destination.SignInversionColumns.Contains(x))
                {
                    sq.Select.Add().Value($"{e.AliasName}.{x} * -1").As(x);
                }
                else
                {
                    sq.Select.Add().Column(e.AliasName, x).As(x);
                }
            });
        };

        var addSelectHeaderColumn = () =>
        {
            //header
            var header = ds.Destination.HeaderDestination;
            if (header == null) return;
            var h = e.InnerJoin(header.TableFulleName).As("h").On(header.SequenceConfig.Column);
            var dscols = sq.GetSelectItems().Select(x => x.Name);
            header.GetInsertColumns().Where(x => !dscols.Contains(x)).ToList()
                .ForEach(x => sq.Select.Add().Column(h, x));
        };

        //select
        sq.Select(bridge, ds.Destination.GetOffsetIdColumnName(config.OffsetConfig)).As(ds.Destination.SequenceConfig.Column);
        addSelectColumn();
        addSelectHeaderColumn();
        var q = sq.ToQuery();
        return q;
    }

    internal static Query BuildSelectOffsetExtensionFromBridge(Datasource ds, string bridgeName, Destination extension, SystemConfig config)
    {
        /*
         * select e.value1, e.value2 * -1 as value2
         * from extensions as e
         * inner join desitination_ext as ext on e.extension_id = ext.id
         * inner join bridge on ext.destination_id = bridge.destination_id
         */
        var table = ds.Destination.GetExtendTableName(config.ExtendConfig);

        //extend
        var sq = new SelectQuery();
        var e = sq.From(extension.TableFulleName).As("e");
        var ext = e.InnerJoin(table).As("ext").On(extension.SequenceConfig.Column, "id");
        ext.InnerJoin(bridgeName).As("bridge").On(ds.Destination.SequenceConfig.Column);

        var addSelectColumn = () =>
        {
            var hseq = extension.HeaderDestination?.SequenceConfig.Column;

            var cols = extension.GetInsertColumns().Where(x => x != hseq).ToList();
            cols.ForEach(x =>
            {
                if (extension.SignInversionColumns.Contains(x))
                {
                    sq.Select.Add().Value($"{e.AliasName}.{x} * -1").As(x);
                }
                else
                {
                    sq.Select.Add().Column(e.AliasName, x).As(x);
                }
            });

            sq.Select.Add().Column(ext, "id");
        };

        var addSelectHeaderColumn = () =>
        {
            //header
            var header = extension.HeaderDestination;
            if (header == null) return;
            var h = e.InnerJoin(header.TableFulleName).As("h").On(header.SequenceConfig.Column);
            var dscols = sq.GetSelectItems().Select(x => x.Name);
            header.GetInsertColumns().Where(x => !dscols.Contains(x)).ToList()
                .ForEach(x => sq.Select.Add().Column(h, x));
        };

        //select
        addSelectColumn();
        addSelectHeaderColumn();

        var q = sq.ToQuery();

        return q;
    }

    internal static Query BuildSelectOffsetDestinationIdsFromBridge(Datasource ds, string bridgeName, SystemConfig config)
    {
        var table = ds.Destination.GetExtendTableName(config.ExtendConfig);

        //extend
        var sq = new SelectQuery();
        var e = sq.From(table).As("e");
        var b = e.InnerJoin(bridgeName).As("b").On(ds.Destination.SequenceConfig.Column);
        sq.Distinct();
        sq.Select.Add().Column(e, "destination_id");
        var q = sq.ToQuery();

        return q;
    }

    internal static Query BuildInsertOffsetMap(Datasource ds, string bridgeName, SystemConfig config)
    {
        /*
          * select destination_id, offset_id, renewal_id, remarks
          * from bridge
          */

        var offset = ds.Destination.GetOffsetTableName(config.OffsetConfig);

        //from bridge
        var sq = new SelectQuery();
        var bridge = sq.From(bridgeName).As("bridge");

        //select destination_id, offset_id, renewal_id, remarks
        sq.Select(bridge, ds.Destination.SequenceConfig.Column);
        sq.Select(bridge, ds.Destination.GetOffsetIdColumnName(config.OffsetConfig));
        sq.Select(bridge, ds.Destination.GetRenewIdColumnName(config.OffsetConfig));
        sq.Select(bridge, config.OffsetConfig.OffsetRemarksColumn);

        var q = sq.ToInsertQuery(offset, new());
        return q;
    }

    internal static Query BuildDeleteKeyMapFromBridge(Datasource ds, string bridgeName, SystemConfig config)
    {
        /*
         * delete from keymap
         * where 
         *   exists (select * from bridge where bridge.destination_id = map.destination_id)
         */

        var map = ds.GetKeymapTableName(config.KeyMapConfig);
        if (map == null) throw new InvalidProgramException("keymaptable is not found.");

        var w = new ConditionClause("where");
        w.ConditionGroup.Add().Exists(x =>
        {
            x.SelectAll();
            var bridge = x.From(bridgeName).As("bridge");
            var id = ds.Destination.SequenceConfig.Column;
            x.Where.Add().Column(bridge, id).Equal(map, id);
        });

        var q = w.ToQuery();
        q.CommandText = $"delete from {map} {q.CommandText}";

        return q;
    }

    internal static SelectQuery BuildSelectBridgeQuery(Datasource ds, SystemConfig config, Action<SelectQuery, Datasource>? injector)
    {
        var sq = BuildSelectExpectValueFromDestination(ds, config, injector);
        sq = BuildSelectBridgeQuery(sq, ds, config);
        return sq;
    }

    private static SelectQuery BuildSelectExpectValueFromDestination(Datasource ds, SystemConfig config, Action<SelectQuery, Datasource>? injector)
    {
        /*
         * select d.*, datasource_ids
         * from destination d
         * inner join systemtable
         * where 
         *     injection
         */
        var alias = "d";

        var sq = new SelectQuery();
        var d = sq.From(ds.Destination.TableName).As(alias);
        sq.SelectAll(d);

        sq = InjectSelectDatasourceId(sq, ds, config);

        //inject from custom function
        if (injector != null) injector(sq, ds);

        return sq;
    }

    private static SelectQuery InjectSelectDatasourceId(SelectQuery sq, Datasource ds, SystemConfig config)
    {
        /*
         * select m.datasource_ids
         * from destination dinner join keymap _m on d.desitination_id = _m.desitination_id
         * inner join sync _s on d.desitination_id = _s.destination_id
         * inner join kms_processes _p on _s.kms_process_id = _p.kms_process_id
         * inner join kms_transactions _t on _p.kms_transaction_id = _t.kms_transaction_id
         * where 
         *     _t.destinatiom_id = :destinatiom_id
         *     and _t.datasource_id = :datasource_id
         */
        var seq = ds.Destination.SequenceConfig.Column;

        var map = ds.GetKeymapTableName(config.KeyMapConfig);
        var sync = ds.Destination.GetSyncTableName(config.SyncConfig);

        var d = sq.FromClause;
        var m = d.InnerJoin(map).As("_m").On(seq);
        var s = d.InnerJoin(sync).As("_s").On(seq);
        var p = s.InnerJoin("kms_processes").As("_p").On("kms_process_id");
        var t = p.InnerJoin("kms_transactions").As("_t").On("kms_transaction_id");

        ds.KeyColumnsConfig.ForEach(x => sq.Select.Add().Column(m, x.Key));

        sq.Where.Add().Column(t, "destination_id").Equal(":destination_id").AddParameter(":destination_id", ds.Destination.DestinationId);
        sq.Where.Add().Column(t, "datasource_id").Equal(":datasource_id").AddParameter(":datasource_id", ds.DatasourceId);

        return sq;
    }

    private static SelectQuery BuildSelectBridgeQuery(SelectQuery expectquery, Datasource ds, SystemConfig config)
    {
        /*
         * with
         * e as (
         *     expect query
         * ), 
         * select e.destination_id, offset_id, null as renewal_id, d.*, remarks
         * from e
         * left join datasource d on e.datasource_ids = d.datasource_ids
         */
        var sq = expectquery.PushToCommonTable("expect");
        var e = sq.From("expect").As("e");
        var seq = ds.Destination.SequenceConfig;

        //var d = e.LeftJoin(SqlParser.Parse(ds.Query)).As("d").On(x =>
        //{
        //    ds.KeyColumnsConfig.ForEach(col => x.Add().Equal(col.Key));
        //});

        //var dscols = SqlParser.Parse(ds.Query).Select.GetColumnNames();
        //var ignores = ds.Destination.InspectionIgnoreColumns;
        //var cols = ds.Destination.GetInsertColumns().ToList()
        //    .Where(x => dscols.Contains(x))
        //    .Where(x => !ignores.Contains(x)).ToList();

        //select expect.destination_id, offset_id, renewal_id, actual.*, remarks
        sq.Select(e, seq.Column);
        sq.Select.Add().Value(seq.Command).As(ds.Destination.GetOffsetIdColumnName(config.OffsetConfig));
        sq.Select.Add().Value("null::bigint").As(ds.Destination.GetRenewIdColumnName(config.OffsetConfig));
        //sq.SelectAll(d);
        sq.Select.Add().Value("'offset'").As(config.OffsetConfig.OffsetRemarksColumn);

        return sq;
    }

    internal static Datasource BuildOffsetDatasrouce(Datasource ds, string bridgeName, SystemConfig config)
    {
        //virtual datasource
        var q = BuildSelectOffsetDatasourceFromBridge(ds, bridgeName, config);

        var datasource = new Datasource()
        {
            DatasourceId = ds.DatasourceId,
            DatasourceName = ds.DatasourceName,
            Destination = ds.Destination,
            DestinationId = ds.Destination.DestinationId,
            Query = q.CommandText
        };
        return datasource;
    }

    internal static Datasource BuildOffsetExtensionDatasrouce(Datasource ds, Destination extension, string bridgeName, SystemConfig config)
    {
        //virtual datasource
        var q = BuildSelectOffsetExtensionFromBridge(ds, bridgeName, extension, config);

        var datasource = new Datasource()
        {
            DatasourceId = ds.DatasourceId,
            DatasourceName = ds.DatasourceName,
            Destination = ds.Destination,
            DestinationId = ds.Destination.DestinationId,
            Query = q.CommandText
        };
        return datasource;
    }

    internal static Query BuildInsertOffsetMapExtension(Datasource ds, string bridgeName, SystemConfig config)
    {
        /*
          * select id as destination_id, destination_id as offset_id, null as renewal_id, 'offset' as remarks
          * from bridge
          */

        var offset = ds.Destination.GetOffsetTableName(config.OffsetConfig);

        //from bridge
        var sq = new SelectQuery();
        var bridge = sq.From(bridgeName).As("bridge");

        //select destination_id, offset_id, renewal_id, remarks
        sq.Select.Add().Column(bridge, "id").As(ds.Destination.SequenceConfig.Column);
        sq.Select.Add().Column(bridge, ds.Destination.SequenceConfig.Column).As(ds.Destination.GetOffsetIdColumnName(config.OffsetConfig));
        sq.Select.Add().Value("null::bigint").As(ds.Destination.GetRenewIdColumnName(config.OffsetConfig));
        sq.Select.Add().Value("'offset'").As(config.OffsetConfig.OffsetRemarksColumn);

        var q = sq.ToInsertQuery(offset, new());
        return q;
    }
}
