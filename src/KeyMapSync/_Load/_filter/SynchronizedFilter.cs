using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load;

public class SynchronizedFilter : IParameterSettable
{
    public IDatasource Datasource { get; set; }

    /// <summary>
    /// ex.sales_map_shop_sales
    /// </summary>
    public string KeyMapTableName { get; set; }

    /// <summary>
    /// ex.km
    /// </summary>
    public string KeyMapAliasName { get; set; }

    public ParameterSet ToParameterSet()
    {
        if (string.IsNullOrEmpty(KeyMapTableName)) throw new InvalidOperationException($"{nameof(KeyMapTableName)} is required.");
        if (string.IsNullOrEmpty(KeyMapAliasName)) throw new InvalidOperationException($"{nameof(KeyMapAliasName)} isrequired.");
        
        var keys = Datasource.DatasourceKeyColumns;
        var alias = Datasource.AliasName;

        var sql = $"exists (select * from {KeyMapTableName} {KeyMapAliasName} where {alias}.{keys.Select(x => $"{alias}.{x} = {KeyMapAliasName}.{x}")})";
        return new ParameterSet() { ConditionSqlText = sql };
    }
}
