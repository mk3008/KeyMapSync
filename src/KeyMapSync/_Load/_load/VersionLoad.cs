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

public class VersionLoad : ILoad
{
    public DbManager Manager { get; set; }

    public string DestinationTableName { get; set; }

    public string DatasourceNameColumnName { get; set; }

    public string DatasourceName { get; set; }

    public SequenceColumn VersionColumnName { get; set; }

    public int Version { get; private set; }

    public Result Execute()
    {
        if (VersionColumnName == null) throw new InvalidOperationException($"{nameof(VersionColumnName)} is required.");

        if (string.IsNullOrEmpty(DestinationTableName)) throw new InvalidOperationException($"{nameof(DestinationTableName)} is required.");
        if (string.IsNullOrEmpty(DatasourceNameColumnName)) throw new InvalidOperationException($"{nameof(DatasourceNameColumnName)} is required.");
        if (string.IsNullOrEmpty(DatasourceName)) throw new InvalidOperationException($"{nameof(DatasourceName)} is required.");

        //ex.Bridge -> Version
        //insert into Version (datasource_name)
        //values (:datasource_name)
        //returning version_id

        Version = Manager.Executor.Connection.ExecuteScalar<int>($"select {VersionColumnName.NextValCommand}");

        var ps = new ParameterSet();
        ps.Parameters.Add(DatasourceNameColumnName, DatasourceNameColumnName);
        ps.Parameters.Add(VersionColumnName.ColumnName, Version);

        var sql =
$@"insert into {DestinationTableName} ({VersionColumnName.ColumnName}, {DatasourceName})
values ({ps.Parameters.Keys.Select(x => $":{x}").ToString(",")});";

        var sw = new Stopwatch();
        sw.Start();
        Version = Manager.Executor.Connection.Execute(sql, ps?.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = DestinationTableName, Count = 1, Elapsed = sw.Elapsed, IsBridge = false };

        return result;
    }
}

