using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Linq;

namespace KeyMapSync.Transform;

public class UnSynchronizedCondition : IFilterable
{
    public Datasource Datasource { get; set; }

    /// <summary>
    /// ex.integration_sales_detail_keymap_ec_shop_sales_detail
    /// </summary>
    public string KeyMapName { get; set; }

    public Filter ToFilter()
    {
        if (Datasource == null) throw new InvalidOperationException($"{nameof(Datasource)} is required.");
        if (string.IsNullOrEmpty(KeyMapName)) throw new InvalidOperationException($"{nameof(KeyMapName)} is required.");

        var keys = Datasource.KeyColumns;
        var ds = Datasource.Alias;

        var sql = $"not exists (select * from {KeyMapName} _km where {ds}.{keys.Select(x => $"{ds}.{x} = _km.{x}").ToString(" and ")})";
        return new Filter() { Condition = sql };
    }
}
