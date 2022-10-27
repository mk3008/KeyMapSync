using KeyMapSync.Entity;
using SqModel;
using SqModel.Analysis;
using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal class InsertQueryBuilder
{
    private static SelectQuery BuildSelectSequenceFromBridge(string bridgeName, Destination dest)
    {
        /*
         * select 
         *     seq
         * from bridge
         */
        var alias = "bridge";

        var sq = new SelectQuery();
        sq.From(bridgeName).As(alias);

        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, dest.SequenceConfig.Column);

        return sq;
    }

    public static SelectQuery BuildSelectValueFromDatasource(Datasource ds, SystemConfig config, bool hasKeymap, Action<SelectQuery, Datasource>? injector)
    {
        /*
         * with
         * d as (
         *     select
         *         generate_sequence as destination_id
         *         , value
         *         , root_id
         *     from
         *         datasource query
         * ) 
         * select 
         *     destination_id
         *     , value
         *     , root_id
         * from d
         */
        var alias = "d";

        var sq = SqlParser.Parse(ds.Query);

        //auto fix keycolumns
        if (ds.KeyColumnsConfig.Any())
        {
            var f = sq.FromClause;
            var c = sq.GetSelectItems().Select(x => x.Name).ToList();
            var keys = ds.KeyColumnsConfig.Select(x => x.Key).ToList();
            keys.Where(x => !c.Contains(x)).ToList().ForEach(x => sq.Select.Add().Column(f, x));
        }

        //cascade root_id
        if (ds.Destination.IsHeader == false && !ds.IsRoot)
        {
            var f = sq.FromClause;
            sq.Select.Add().Column(f, "root_id").As("root_id");
        }

        //seq
        var seq = ds.Destination.SequenceConfig;
        if (!sq.Select.GetColumnNames().Contains(seq.Column))
        {
            sq.Select(seq.Command).As(seq.Column);
        }

        if (hasKeymap) InjectNotSyncCondition(sq, ds, config);

        //inject from custom function
        if (injector != null) injector(sq, ds);

        //clean
        sq = sq.PushToCommonTable(alias);
        var d = sq.From(alias);

        //select root_id
        if (ds.Destination.IsHeader == false && ds.IsRoot)
        {
            var f = sq.FromClause;
            sq.Select.Add().Column(f, seq.Column).As("root_id");
        }

        //select CTE columns
        var cols = sq.GetCommonTableClauses().Where(x => x.Name == "d").First().Query.GetSelectItems();
        cols.ForEach(x => sq.Select.Add().Column(d, x.Name));

        return sq;
    }

    private static void InjectNotSyncCondition(SelectQuery sq, Datasource ds, SystemConfig config)
    {
        /*
         * left join map m on d.destination_id = m.destination_id
         * where m.destination_id is null
         */
        var mapname = ds.GetKeymapTableName(config.KeyMapConfig);

        var d = sq.FromClause;
        var m = sq.FromClause.LeftJoin(mapname).As("m").On(x =>
        {
            ds.KeyColumnsConfig.ForEach(y => x.Add().Equal(y.Key));
        });
        sq.Where.Add().Column(m, ds.KeyColumnsConfig.First().Key).IsNull();
    }

    public static Query BuildInsertKeymapFromBridge(Datasource ds, string bridgeName, SystemConfig config)
    {
        /*
         * insert into keymap (destination_id, datasource_key)
         * select 
         *     destination_id
         *     , datasource_key
         * from bridge
         */
        var sq = BuildSelectSequenceFromBridge(bridgeName, ds.Destination);
        var t = sq.FromClause;

        //select
        ds.KeyColumnsConfig.ForEach(x => sq.Select(t, x.Key));

        var mapname = ds.GetKeymapTableName(config.KeyMapConfig);
        var q = sq.ToInsertQuery(mapname, new());

        return q;
    }

    public static Query BuildInsertDestinationFromBridge(Datasource ds, string bridgeName, List<string> bridgeCols)
    {
        /*
         * insert into destination (destination_id, value)
         * select 
         *     destination_id
         *     , value
         * from bridge
         */
        var sq = BuildSelectSequenceFromBridge(bridgeName, ds.Destination);
        var bridge = sq.FromClause;

        //select
        ds.Destination.GetInsertColumns().Where(x => bridgeCols.Contains(x)).ToList().ForEach(x => sq.Select(bridge, x));

        var q = sq.ToInsertQuery(ds.Destination.TableName, new());

        return q;
    }

    public static Query BuildeInsertSyncFromBridge(long procid, Datasource ds, string bridgeName, SystemConfig config)
    {
        /*
         * insert into sync (destination_id, process_id)
         * select 
         *     destination_id
         *     , :process_id as process_id
         * from tmp bridge
         */
        var sq = InsertQueryBuilder.BuildSelectSequenceFromBridge(bridgeName, ds.Destination);
        sq.Select.Add().Value(":process_id").As("kms_process_id").AddParameter(":process_id", procid);

        var syncname = ds.Destination.GetSyncTableName(config.SyncConfig);
        var q = sq.ToInsertQuery(syncname, new());
        return q;
    }

    public static Query BuildCountQuery(string table)
    {
        var sq = new SelectQuery();
        sq.From(table);
        sq.SelectCount();
        return sq.ToQuery();
    }

    public static Query BuildInsertExtFromBridge(Datasource ds, string bridgeName, SystemConfig config)
    {
        var tbl = ds.Destination.GetExtendTableName(config.ExtendConfig);
        if (tbl == null) throw new Exception($"extend table is not available.(Destination : {ds.Destination.DestinationName})");
        /*
         * insert into ext (base_id, table_name, id)
         * select 
         *     base_id
         *     , table_name
         *     , id
         * from bridge
         */

        var sq = new SelectQuery();
        var bridge = sq.From(bridgeName).As("bridge");

        var baseds = ds.BaseDatasource;
        if (baseds == null) throw new Exception();

        var seq = baseds.Destination.SequenceConfig;

        //select
        sq.Select.Add().Column(bridge, $"root_id").As(seq.Column);
        sq.Select.Add().Value(":dest_id").As("destination_id").AddParameter(":dest_id", ds.Destination.DestinationId);
        sq.Select.Add().Value(":table_name").As("extension_table_name").AddParameter(":table_name", ds.Destination.TableFulleName);
        sq.Select.Add().Column(bridge, ds.Destination.SequenceConfig.Column).As("id");

        var q = sq.ToInsertQuery(tbl, new());
        return q;
    }
}
