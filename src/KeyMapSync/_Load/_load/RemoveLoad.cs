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

public class RemoveLoad : ILoad
{
    public DbManager Manager { get; set; }

    public string RemoveTableName { get; set; }

    public string AliasName { get; set; }

    public ParameterSet ParameterSet { get; set; }

    public IList<ILoad> Loads => new List<ILoad>();

    public Result Execute()
    {
        if (ParameterSet == null) throw new InvalidOperationException($"{nameof(ParameterSet)} is required.");

        if (string.IsNullOrEmpty(RemoveTableName)) throw new InvalidOperationException($"{nameof(RemoveTableName)} is required.");
        if (string.IsNullOrEmpty(AliasName)) throw new InvalidOperationException($"{nameof(AliasName)} is required.");
  
        var ps = ParameterSet ?? new ParameterSet();

        //ex.OFFSET_BRIDGE -> KEYMAP
        //delete from KEYMAP
        //where exists (select * from OFFSET_BRIDGE where KEYMAP.table_id = BRIDGE.table_id)

        var sql =
$@"delete from {RemoveTableName} {AliasName}
{ps.ToWhereSqlText()}";

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps?.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = RemoveTableName, Count = cnt * -1, Elapsed = sw.Elapsed, IsBridge = false };

        //Cannot be parallelized because it shares transactions.
        foreach (var item in Loads)
        {
            result.InnerResults.Add(item.Execute());
        }

        return result;
    }
}