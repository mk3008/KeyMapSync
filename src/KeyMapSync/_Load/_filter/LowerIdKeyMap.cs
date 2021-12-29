using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Load;

internal class LowerIdKeyMap : IParameterSettable
{
    /// <summary>
    /// ex.sales_map_shop_sales
    /// </summary>
    public string KeyMapTableName { get; set; }

    /// <summary>
    /// ex.km
    /// </summary>
    public string KeyMapAliasName { get; set; }

    /// <summary>
    /// ex.datasource
    /// </summary>
    public string DatasourceAliasName { get; set; }

    /// <summary>
    /// ex.new string [] {"shop_sales_id"}
    /// </summary>
    public IEnumerable<string> KeyColumns { get; set; }

    public ParameterSet ToParameterSet()
    {
        if (string.IsNullOrEmpty(KeyMapTableName)) throw new InvalidOperationException($"{nameof(KeyMapTableName)} is required.");
        if (string.IsNullOrEmpty(KeyMapAliasName)) throw new InvalidOperationException($"{nameof(KeyMapAliasName)} isrequired.");
        if (string.IsNullOrEmpty(DatasourceAliasName)) throw new InvalidOperationException($"{nameof(DatasourceAliasName)} is required.");
        if (KeyColumns.Count() != 1) throw new InvalidOperationException($"The count of {nameof(KeyColumns)} is only one.");

        var sql = $"{DatasourceAliasName}.{KeyColumns.First()} > (select {KeyColumns.First()} from {KeyMapTableName} order by {KeyColumns.First()} desc limit 1)";
        return new ParameterSet() { ConditionSqlText = sql };
    }
}

