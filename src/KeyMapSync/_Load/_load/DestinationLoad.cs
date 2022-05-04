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

public class DestinationLoad : ILoad
{
    public DbManager Manager { get; set; }

    public string BridgeTableName { get; set; }

    public string BridgeAliasName { get; set; }

    public string DestinationTableName { get; set; }

    public VersionLoad VersionLoad { get; set; }

    public ParameterSet BridgeFilter { get; set; }

    public IList<ILoad> Cascades => new List<ILoad>();

    public Result Execute()
    {
        if (Manager == null) throw new InvalidOperationException($"{nameof(Manager)} is required.");
        if (BridgeFilter == null) throw new InvalidOperationException($"{nameof(BridgeFilter)} is required.");

        if (string.IsNullOrEmpty(BridgeTableName)) throw new InvalidOperationException($"{BridgeTableName} is required.");
        if (string.IsNullOrEmpty(BridgeAliasName)) throw new InvalidOperationException($"{BridgeAliasName} is required.");
        if (string.IsNullOrEmpty(DestinationTableName)) throw new InvalidOperationException($"{DestinationTableName} required.");

        var ps = BridgeFilter ?? new ParameterSet();

        //ex.Bridge -> Destination
        //  insert into Destination (destination_id, name, value)
        //  select destination_id, name, value
        //  from Bridge
        //  where BridgeFilter

        var ver = VersionLoad.Version;

        // Runtime scan columns.
        var dscols = Manager.ReadTable(BridgeTableName).Columns;
        var destcols = Manager.ReadTable(DestinationTableName)?.Columns;
        var cols = dscols.Where(x => destcols.Contains(x));

        var sql =
$@"insert into {DestinationTableName} ({cols.ToString(",")})
select {cols.ToString(",")}
from {BridgeTableName} {BridgeAliasName}
{ps?.ToWhereSqlText()};";

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = DestinationTableName, Count = cnt, Elapsed = sw.Elapsed, IsBridge = false };

        if (cnt == 0) return result;

        //Cannot be parallelized because it shares transactions.
        foreach (var item in Cascades)
        {
            result.InnerResults.Add(item.Execute());
        }

        return result;
    }
}
