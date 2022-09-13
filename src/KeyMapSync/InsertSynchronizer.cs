using KeyMapSync.Entity;
using SqModel.Analysis;
using SqModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqModel.Dapper;

namespace KeyMapSync;

public class InsertSynchronizer
{
    private InsertSynchronizer(InsertSynchronizer owner, Datasource datasource, Action<SelectQuery> injector)
    {
        Connection = owner.Connection;
        Datasource = datasource;
        Timeout = owner.Timeout;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(datasource.DatasourceName).Substring(0, 4);
        BridgeName = $"{owner.BridgeName}_{tmp}";
    }

    public InsertSynchronizer(IDbConnection connection, Datasource datasource, Action<SelectQuery>? injector = null)
    {
        Connection = connection;
        Datasource = datasource;
        Injector = injector;

        var tmp = BridgeNameBuilder.GetName(datasource.DatasourceName).Substring(0, 4);
        BridgeName = $"_{tmp}";
    }

    public Action<string>? Logger { get; set; } = null;

    public int? Timeout { get; set; } = null;

    private IDbConnection Connection { get; init; }

    private Action<SelectQuery>? Injector { get; init; }

    private Datasource Datasource { get; init; }

    private string BridgeName { get; init; }

    private Destination Destination => Datasource.Destination;

    private KeyMapConfig? KeyMapConfig => Destination.KeyMapConfig;

    private VersioningConfig? VersioningConfig => Destination.VersioningConfig;

    private VersionConfig? VersionConfig => Destination.VersioningConfig?.VersionConfig;

    private SelectQuery CreateSelectFromBridgeQuery()
    {
        var alias = "bridge";

        var sq = new SelectQuery();
        sq.From(BridgeName).As(alias);

        return sq;
    }

    private SelectQuery CreateSelectFromBridgeQueryAsAdditional()
    {
        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, Destination.Sequence.Column);

        return sq;
    }

    private int ExecuteQuery(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var cnt = Connection.Execute(q, commandTimeout: Timeout);
        Logger?.Invoke($"--count : {cnt}");

        return cnt;
    }

    private T ExecuteScalar<T>(Query q)
    {
        Logger?.Invoke(q.ToDebugString());

        var val = Connection.ExecuteScalar<T>(q, commandTimeout: Timeout);
        Logger?.Invoke($"--results : {val?.ToString()}");

        return val;
    }

    public Results Insert()
    {
        Logger?.Invoke($"--insert {Destination.TableName} <- {Datasource.DatasourceName}");

        var sq = BuildSelectBridgeQuery();
        return Insert(sq);
    }

    private Results Insert(SelectQuery bridgequery)
    {
        var results = new Results();

        var cnt = CreateBridgeTable(bridgequery);
        if (cnt == 0) return results;

        if (InsertDestination(bridgequery) != cnt) throw new InvalidOperationException();
        results.Add(new Result() { Table = Destination.TableName, Count = cnt });

        if (KeyMapConfig != null)
        {
            if (InsertKeyMap() != cnt) throw new InvalidOperationException();
            results.Add(new Result() { Table = Datasource.GetKeymapTableName(), Count = cnt });
        }
        if (VersioningConfig != null)
        {
            if (InsertSync() != cnt) throw new InvalidOperationException();
            results.Add(new Result() { Table = Datasource.GetSyncTableName(), Count = cnt });
        }
        if (VersionConfig != null)
        {
            if (InsertVersion(bridgequery) != 1) throw new InvalidOperationException();
            results.Add(new Result() { Table = Datasource.GetVersionTableName(), Count = 1 });
        }

        //nest
        Datasource.Extensions.ForEach(x =>
        {
            //replace root table injector
            Action<SelectQuery> act = q => q.FromClause.TableName = BridgeName;

            var s = new InsertSynchronizer(this, x, act) { Logger = Logger };
            results.Add(s.Insert());
        });
        return results;
    }

    private SelectQuery BuildSelectBridgeQuery()
    {
        /*
         * with
         * d as (
         *     --datasource
         * )
         * select 
         *     generate_sequence as destination_id
         *     , d.*
         *     , v.version_id
         * from d
         * cross join (select generate_sequence as version_id) v
         * left join map m on d.destination_id = m.destination_id
         * where m.destination_id is null
         */
        var alias = "d";

        var sq = SqlParser.Parse(Datasource.Query);
        var cols = sq.SelectClause.GetColumnNames();

        //inject from custom function
        if (Injector != null) Injector(sq);

        sq = sq.PushToCommonTable(alias);

        //from
        var d = sq.From(alias);

        //select
        var seq = Destination.Sequence;
        sq.Select(seq.Command).As(seq.Column);
        cols.ForEach(x => sq.Select.Add().Column(d, x));

        //inject from config
        sq = InjectSelectVersion(sq);
        sq = InjectNotSyncCondition(sq);

        return sq;
    }

    private SelectQuery InjectSelectVersion(SelectQuery sq)
    {
        var alias = "v";

        var config = VersioningConfig;
        if (config == null) return sq;

        var seq = config.Sequence;
        sq.FromClause.CrossJoin(q => q.Select(seq.Command).As(seq.Column)).As(alias);
        sq.Select.Add().Column(alias, seq.Column);

        return sq;
    }

    private SelectQuery InjectNotSyncCondition(SelectQuery sq)
    {
        var alias = "m";

        var map = Datasource.GetKeymapTableName();
        if (map == null) return sq;

        var d = sq.FromClause;
        sq.FromClause.LeftJoin(map).As(alias).On(x =>
        {
            Datasource.KeyColumns.ForEach(y => x.Add().Equal(y.Key));
        });
        sq.Where.Add().Column(alias, Datasource.KeyColumns.First().Key).IsNull();
        return sq;
    }

    private int CreateBridgeTable(SelectQuery sq)
    {
        var q = sq.ToCreateTableQuery(BridgeName, true);
        ExecuteQuery(q);

        q = BuildBridgeCountQuery();
        return ExecuteScalar<int>(q);
    }

    private Query BuildBridgeCountQuery()
    {
        var sq = new SelectQuery();
        sq.From(BridgeName);
        sq.SelectCount();
        return sq.ToQuery();
    }

    private int InsertVersion(SelectQuery bridgequery)
    {
        /*
         * insert into version (version_id, datasource_name, bridge_command)
         * select distinct 
         *     version_id
         *     , :name as datasource_name
         *     , :query as bridge_command
         * from tmp bridge
         */
        if (VersioningConfig == null) return 0;
        if (VersionConfig == null) return 0;

        var ver = Datasource.GetVersionTableName();
        if (ver == null) return 0;

        var sq = CreateSelectFromBridgeQuery();
        var bridge = sq.FromClause;

        //select
        sq.Distinct();
        sq.Select(bridge, VersioningConfig.Sequence.Column);
        sq.Select(":name").As(VersionConfig.DatasourceNameColumn).Parameter(":name", Datasource.DatasourceName);
        sq.Select(":query").As(VersionConfig.BridgeCommandColumn).Parameter(":query", bridgequery.ToQuery().CommandText);

        var q = sq.ToInsertQuery(ver);

        return ExecuteQuery(q);
    }

    private int InsertSync()
    {
        /*
         * insert into sync (destination_id, version_id)
         * select 
         *     destination_id
         *     , version_id
         * from tmp bridge
         */

        if (VersioningConfig == null) return 0;

        var sync = Datasource.GetSyncTableName();
        if (sync == null) return 0;

        var sq = CreateSelectFromBridgeQueryAsAdditional();
        var bridge = sq.FromClause;

        //select
        sq.Select(bridge, VersioningConfig.Sequence.Column);

        var q = sq.ToInsertQuery(sync);
        return ExecuteQuery(q);
    }

    private int InsertKeyMap()
    {
        /*
         * insert into map (destination_id, datasource_id)
         * select 
         *     destination_id
         *     , datasource_id
         * from tmp bridge
         */

        var map = Datasource.GetKeymapTableName();
        if (map == null) return 0;

        var sq = CreateSelectFromBridgeQueryAsAdditional();
        var bridge = sq.FromClause;

        //select
        Datasource.KeyColumns.ForEach(x => sq.Select(x.Key).As(x.Key));

        var q = sq.ToInsertQuery(map);
        return ExecuteQuery(q);
    }

    private int InsertDestination(SelectQuery bridgequery)
    {
        /*
         * insert into destination (destination_id, value)
         * select 
         *     destination_id
         *     , value
         * from tmp bridge
         */

        var sq = CreateSelectFromBridgeQueryAsAdditional();
        var bridge = sq.FromClause;

        //select
        var cols = bridgequery.SelectClause.GetColumnNames().ToList();
        Destination.GetInsertColumns().Where(x => cols.Contains(x)).ToList().ForEach(x => sq.Select(bridge, x));

        var q = sq.ToInsertQuery(Destination.TableName);
        return ExecuteQuery(q);
    }
}