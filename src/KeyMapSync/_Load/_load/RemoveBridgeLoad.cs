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

public class RemoveBridgeLoad : ILoad
{
    public DbManager Manager { get; set; }

    /// <summary>
    /// Reload bridge.
    /// </summary>
    public TableMap ExpectBridge { get; set; }

    public string RemoveBridgeTableName { get; set; }

    public TableMap Destination { get; set; }

    public SequenceMap DestinationSequence { get; set; }

    public string DestinationKeyColumnName { get; set; }

    public string ExpectBridgeKeyColumnName { get; set; }

    public IEnumerable<string> IgnoreColumns { get; set; }

    public IEnumerable<string> ValueColumns { get; set; }

    public IValidateFilter ValidateFilter { get; set; }

    public IList<ILoad> Loads => new List<ILoad>();

    public Result Execute()
    {
        if (ExpectBridge == null) throw new InvalidOperationException($"{nameof(ExpectBridge)} is required.");
        if (Destination == null) throw new InvalidOperationException($"{nameof(Destination)} is required.");

        if (string.IsNullOrEmpty(DestinationKeyColumnName)) throw new InvalidOperationException($"{nameof(DestinationKeyColumnName)} is required.");
        if (string.IsNullOrEmpty(ExpectBridgeKeyColumnName)) throw new InvalidOperationException($"{nameof(ExpectBridgeKeyColumnName)} is required.");
        if (string.IsNullOrEmpty(RemoveBridgeTableName)) throw new InvalidOperationException($"{nameof(RemoveBridgeTableName)} required.");

        //ex.EXPECT_BRIDGE -> REMOVE_BRIDGE (-> REMOVE_DESTINATION, OFFSET_KEYMAP)
        //
        //  create temporary table REMOVE_BRIDGE
        //  as
        //  select SEQUENCE_FUNCTION as desination_id, EXPECT_BRIDGE._origin_desination_id, EXPECT_BRIDGE.table_id, EXPECT_BRIDGE.name, DESTINATION.value * -1 as value, EXPECT_BRIDGE.extension_value
        //  from DESTINATION
        //  left join EXPECT_BRIDGE on DESTINATION.desination_id = EXPECT_BRIDGE._origin_desination_id
        //  where ((EXPECT_BRIDGE._origin_desination_id is null) or (DESTINATION.name <> EXPECT_BRIDGE.name) or (DESTINATION.value <> EXPECT_BRIDGE.value))

        // Runtime scan columns.
        var bridgecols = Manager.ReadColumns(ExpectBridge.TableName);
        var destcols = Manager.ReadColumns(Destination.TableName);

        var validationcols = destcols.Where(x => !IgnoreColumns.Contains(x)).Where(x => x != DestinationKeyColumnName);

        var ps = LoadParameterSet ?? new ParameterSet();
        ps = ps.Merge(GetRemoveParameterSet(validationcols));

        var sql =
$@"create table {RemoveBridgeTableName}
as 
select {DestinationSequence.ColumnQueryText}, {GetSelectColumns(bridgecols).ToString(",")}, {GetRemoveRemarksColumn(validationcols)}
from {Destination.TableQuery}
left join {ExpectBridge.TableQuery} on {Destination.AliasName}.{DestinationKeyColumnName} = {ExpectBridge.AliasName}.{ExpectBridgeKeyColumnName}
{ps.ToWhereSqlText()};";

        var sw = new Stopwatch();
        sw.Start();
        var cnt = Manager.Executor.Connection.Execute(sql, ps?.ToExpandObject());
        sw.Stop();

        var result = new Result() { Destination = RemoveBridgeTableName, Count = cnt, Elapsed = sw.Elapsed, IsBridge = true };

        //Cannot be parallelized because it shares transactions.
        foreach (var item in Loads)
        {
            result.InnerResults.Add(item.Execute());
        }

        return result;
    }

    private IEnumerable<string> GetSelectColumns(IEnumerable<string> bridgecols)
    {
        var q1 = bridgecols.Where(x => !ValueColumns.Contains(x)).Select(x => $"{ExpectBridge.AliasName}.{x}");
        var q2 = ValueColumns.Select(x => $"{Destination.AliasName}.{x} * -1 as {x}");
        return q1.Union(q2);
    }

    private string GetRemoveRemarksColumn(IEnumerable<string> validationcols)
    {
        var del = $"when {ExpectBridge.AliasName}.{ExpectBridgeKeyColumnName} is null then 'deleted.'";
        var diffs = validationcols.Select(x => $"when not {ExpectBridge.AliasName}.{x} is null and {Destination.AliasName}.{x} is null) and not ({ExpectBridge.AliasName}.{x} = {Destination.AliasName}.{x}) then '{x} is different.'");

        return $"case {del} {diffs.ToString(" ")} end as offset_remarks";
    }

    private ParameterSet GetRemoveParameterSet(IEnumerable<string> validationcols)
    {
        var del = $"({ExpectBridge.AliasName}.{ExpectBridgeKeyColumnName} is null)";
        var diffs = validationcols.Select(x => $"(not {ExpectBridge.AliasName}.{x} is null and {Destination.AliasName}.{x} is null) and not ({ExpectBridge.AliasName}.{x} = {Destination.AliasName}.{x}))");

        var prm = new ParameterSet() { ConditionSqlText = $"({del} or {diffs.ToString(" or ")})" };
        return prm;
    }
}

