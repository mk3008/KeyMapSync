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

public class BridgeLoad : ILoad
{
    public DbManager Manager { get; set; }

    public IDatasource Datasource { get; set; }

    public SequenceColumn DestinationSequence { get; set; }

    public string BridgeTableName { get; set; }

    public UnSynchronizedFilter UnSynchronizedFilter { get; set; }

    public IList<ILoad> Loads => new List<ILoad>();

    public Result Execute()
    {
        if (Datasource == null) throw new InvalidOperationException($"{nameof(Datasource)} is required.");
        if (UnSynchronizedFilter == null) throw new InvalidOperationException($"{nameof(UnSynchronizedFilter)} is required.");

        if (string.IsNullOrEmpty(Datasource.WithQueryText)) throw new InvalidOperationException($"{nameof(Datasource.WithQueryText)} is required.");
        if (string.IsNullOrEmpty(Datasource.AliasName)) throw new InvalidOperationException($"{nameof(Datasource.AliasName)} is required.");
        if (string.IsNullOrEmpty(BridgeTableName)) throw new InvalidOperationException($"{nameof(BridgeTableName)} required.");

        var ps = UnSynchronizedFilter.ToParameterSet() ?? new ParameterSet();
        ps.ConditionSqlText = $"";
        ps = ps.Merge(Datasource.ParameterSet);

        //ex.Datasource -> Bridge (-> Destination)
        //  with Datasource as (select datasource_id, name, value, extension_value from datasource_table)
        //  create temporary table Bridge
        //  as
        //  select SequenceCommand as desination_id, Datasource.*
        //  from Datasource
        //  where not exists (select * from dest_map_table KEYMAP km where Datasource.table_id = Keymap.table_id)

        var seq = DestinationSequence;
        var sql =
$@"{Datasource.WithQueryText}
create temporary table {BridgeTableName}
as 
select {seq.NextValCommand} as {seq.ColumnName}, {Datasource.AliasName}.* from {Datasource.AliasName}
{ps.ToWhereSqlText()};";

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps?.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = BridgeTableName, Count = cnt, Elapsed = sw.Elapsed, IsBridge = true };

        if (cnt ==0) return result;

        //Cannot be parallelized because it shares transactions.
        foreach (var item in Loads)
        {
            result.InnerResults.Add(item.Execute());
        }

        return result;
    }
}

