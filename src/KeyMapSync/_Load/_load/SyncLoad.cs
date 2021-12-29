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

public class SyncLoad : ILoad
{
    public DbManager Manager { get; set; }

    public string SyncTableName { get; set; }

    public string BridgeTableName { get; set; }

    public string DestinationIdColumnName { get; set; }

    public VersionLoad VersionLoad { get; set; }

    public ParameterSet BridgeFilter { get; set; }


    public Result Execute()
    {
        if (Manager == null) throw new InvalidOperationException($"{nameof(Manager)} is required.");
        if (VersionLoad == null) throw new InvalidOperationException($"{nameof(VersionLoad)} is required.");

        if (string.IsNullOrEmpty(BridgeTableName)) throw new InvalidOperationException($"{BridgeTableName} is required.");
        if (string.IsNullOrEmpty(DestinationIdColumnName)) throw new InvalidOperationException($"{DestinationIdColumnName} is required.");
        if (string.IsNullOrEmpty(SyncTableName)) throw new InvalidOperationException($"{SyncTableName} required.");

        //ex.BRIDGE -> SYNC
        //  insert into Sync (desination_id, version_id)
        //  select desination_id, :version_id
        //  from Bridge

        var ps = new ParameterSet();
        var verIdcolumn = VersionLoad.VersionColumnName.ColumnName;
        ps.Parameters.Add(verIdcolumn, VersionLoad.Version);

        var sql =
$@"insert into {SyncTableName} ({DestinationIdColumnName}, {verIdcolumn})
select {DestinationIdColumnName}, :{verIdcolumn}
from {BridgeTableName}";

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = SyncTableName, Count = cnt, Elapsed = sw.Elapsed, IsBridge = false };

        return result;
    }
}
