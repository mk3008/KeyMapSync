using KeyMapSync.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load;

public class SyncTableMap
{
    public string SyncTableName { get; set; }

    public string AliasName { get; set; }

    public string DestinationKeyColumnName { get; set; }

    public int LowerVersionId { get; set; }

    public int HigherVersionId { get; set; }

    /// <summary>
    /// ex."version_id between :_lower_id and _higher_id"
    /// </summary>
    public ParameterSet ParameterSet { get; set; }
}