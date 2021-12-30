using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class BridgeRoot : IBridge
{
    /// <summary>
    /// ex.
    /// with ds as (select * from ec_shop_sale_detail)
    /// </summary>
    public Datasource Datasource { get; set; }

    public IBridge Owner => null;

    /// <summary>
    /// ex."ds"
    /// </summary>
    public string Alias => Datasource.Alias;

    /// <summary>
    /// ex."tmp01"
    /// </summary>
    public string BridgeName { get; set; }

    public IFilter Filter => null;

    public string BuildExtendWithQuery()
    {
        return null;
    }

    /// <summary>
    /// ex.
    /// --unsync filter and sequencing
    /// , _ds as (select SEQ_COMMAND as integration_sales_detail_id, ds.* from _ds where not exists (select * from map where map.ec_shop_sales_detail_id = ds.ec_shop_sales_detail_id) and map.ec_shop_sales_id = ds.ec_shop_sales_id)
    /// --header sequencing
    /// , _head as (select SEQ_COMMAND as integration_sales_id from _ds group by ec_shop_sales_id)
    /// --detail inner join header
    /// , __ds as (select _head.integration_sales_id, _ds.* from _ds inner join _head on _ds.ec_shop_sales_id = _head.ec_shop_sales_id)
    /// </summary>
    public string GetWithQuery() => Datasource.WithQuery;
}

