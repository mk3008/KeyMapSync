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

public class KeyMapLoad : ILoad
{
    public DbManager Manager { get; set; }

    public string BridgeTableName { get; set; }

    public IDatasource Datasource { get; set; }

    public string KeyMapTableName { get; set; }

    public ParameterSet BridgeFilter { get; set; }

    public Result Execute()
    {
        if (Manager == null) throw new InvalidOperationException($"{nameof(Manager)} is required.");
        if (BridgeFilter == null) throw new InvalidOperationException($"{nameof(BridgeFilter)} is required.");

        if (string.IsNullOrEmpty(BridgeTableName)) throw new InvalidOperationException($"{BridgeTableName} is required.");
        if (string.IsNullOrEmpty(BridgeAliasName)) throw new InvalidOperationException($"{BridgeAliasName} is required.");
        if (string.IsNullOrEmpty(KeyMapTableName)) throw new InvalidOperationException($"{KeyMapTableName} required.");

        var ps = BridgeFilter ?? new ParameterSet();

        //ex.Bridge -> Keymap
        //  insert into Keymap (destination_id, datasource_id)
        //  select destination_id, datasource_id
        //  from Bridge
        //  where bridge_filter

        var cols = new List<string>();
        cols.Add(DestinationLoad)

        var sql =
$@"insert into {KeyMapTableName} ({dscols.ToString(",")})
select {dscols.Where(x => destcols.Contains(x)).ToString(",")}
from {BridgeTableName} {BridgeAliasName}
{ps?.ToWhereSqlText()};";

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = KeyMapTableName, Count = cnt, Elapsed = sw.Elapsed, IsBridge = false };

        //Cannot be parallelized because it shares transactions.
        foreach (var item in Loads)
        {
            result.InnerResults.Add(item.Execute());
        }

        return result;
    }
}
