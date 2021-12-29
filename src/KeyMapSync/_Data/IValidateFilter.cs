using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Data;

/// <summary>
/// validate filter. for offset sync.
/// </summary>
public interface IValidateFilter
{

    ParameterSet ToExpectParameterSet(string SyncTableName, string versionColumnName);

    ParameterSet ToRemoveParameterSet(Table dest, Table expectBridge, string bridgeAliasName, string offsetSourceColumnName);

    string ToRemoveCommentColumnSqlText(Table dest, Table expectBridge, string bridgeAliasName, string offsetSourceColumnName, string removeCommentColumnName);
}

