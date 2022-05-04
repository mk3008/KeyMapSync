using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Data;

/// <summary>
/// condition for offset.
/// </summary>
public class OffsetCondition
{
    public string Destination { get; set; }

    public long Lowerversion { get; set; }

    public long UpperLimit { get; set; }

    public IEnumerable<string> KeyColumns { get; set; }

    public IEnumerable<string> IgnoreColumns { get; set; } = Enumerable.Empty<string>();

    public ParameterSet ToTargetParameterSet(string SyncTableName)
    {
        var ps = new ParameterSet();

        //ex. "exists (select * from Sync where Destination.desination_Id = Sync.desination_Id and Sync.Version between :lower and :higher)"
        ps.ConditionSqlText = $"exists (select * from {SyncTableName} _sync where Destination.desination_Id = _syn.desination_Id and Sync.Version between :lower_limit and :upper_limit)";



        return ps;
    }

}

