using Dapper;
using KeyMapSync.Data;
using KeyMapSync.Load;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load;

public class ExpectBridgeLoad : ILoad
{

    public DbManager Manager { get; set; }

    public IDatasource Datasource { get; set; }

    public string DestinationKeyColumnName { get; set; }

    public string ExpectBridgeTableName { get; set; }

    public IValidateFilter ValidateFilter { get; set; }

    public string KeymapTableName { get; set; }

    public string KeymapTableAliasName { get; set; }

    public string SyncTableName { get; set; }

    public string SyncTableAliasName { get; set; }

    public string VersionColumnName { get; set; }

    /// <summary>
    /// ex. "offset_source_"
    /// </summary>
    public string OffsetDataroucePrefix { get; set; }

    public IList<ILoad> Loads => new List<ILoad>();

    public Result Execute()
    {

        var ps = ValidateFilter.ToExpectParameterSet(SyncTableName, VersionColumnName);
        ps = ps.Merge(Datasource.ParameterSet);

        //ex.Datasource -> ExpectBridge (-> RemoveBridge -> Destination)
        //
        //  with Datasource as (select datasource_id, name, value, extension_value from datasource)
        //  create temporary table ExpectBridge (desination_id, table_id, name, value, extension_value)
        //  as 
        //  select Sync.desination_id as offset_source_desination_id, Datasource.*
        //  from Datasource
        //  inner join Keymap on Datasource.datasource_id = Keymap.datasource_id
        //  inner join Sync on Keymap.destination_id = Sync.destination_id
        //  where Sync.version_id between :lower_version_id and :higher_version_id 

        var sourceKeys = Datasource.DatasourceKeyColumns;
        var destKey = DestinationKeyColumnName;

        var sql =
$@"{Datasource.WithQueryText}
create temporary table {ExpectBridgeTableName}
as 
select {SyncTableAliasName}.{destKey} as {OffsetDataroucePrefix}{destKey}, {Datasource.AliasName}.*
from {Datasource.AliasName}
inner join {KeymapTableName} {KeymapTableAliasName} on {sourceKeys.Select(x => $"{Datasource.AliasName}.{x} = {KeymapTableAliasName}.{x}")}
inner join {SyncTableName} {SyncTableAliasName} on {KeymapTableAliasName}.{destKey} = {SyncTableAliasName}.{destKey}
{ps.ToWhereSqlText()}";        

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps?.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = ExpectBridgeTableName, Count = cnt, Elapsed = sw.Elapsed, IsBridge = true };

        //Cannot be parallelized because it shares transactions.
        foreach (var item in Loads)
        {
            result.InnerResults.Add(item.Execute());
        }

        return result;
    }
}

