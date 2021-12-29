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

public class ExtensionInsertLoad : ILoad
{
    public DbManager Manager { get; set; }

    public TableMap BridgeMap { get; set; }

    public string ExtensionTableName { get; set; }

    public IList<ILoad> Loads => new List<ILoad>();

    public Result Execute()
    {
        if (BridgeMap == null) throw new InvalidOperationException($"{nameof(BridgeMap)} is required.");

        if (string.IsNullOrEmpty(BridgeMap.TableName)) throw new InvalidOperationException($"{BridgeMap.TableName} is required.");
        if (string.IsNullOrEmpty(ExtensionTableName)) throw new InvalidOperationException($"{ExtensionTableName} required.");

        var ps = new ParameterSet();

        //ex.BRIDGE -> EXTENSION
        //
        //  insert into EXTENSION (desination_id, extension_value)
        //  select desination_id, extension_value
        //  from BRIDGE
        //  where extension_value is not null

        // Runtime scan columns.
        var destcols = Manager.ReadTable(ExtensionTableName)?.Columns;

        var sql =
$@"insert into {ExtensionTableName} ({destcols.ToString(",")})
select {destcols.Where(x => destcols.Contains(x)).ToString(",")}
from {BridgeMap.TableName} {BridgeMap.AliasName}
{ps?.ToWhereSqlText()};";

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = ExtensionTableName, Count = cnt, Elapsed = sw.Elapsed, IsBridge = false };

        //Cannot be parallelized because it shares transactions.
        foreach (var item in Loads)
        {
            result.InnerResults.Add(item.Execute());
        }

        return result;
    }
}
