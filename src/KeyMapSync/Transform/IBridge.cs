using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public interface IBridge
{
    Datasource Datasource { get; }

    /// <summary>
    /// ex.
    /// --unsync filter and sequencing
    /// , _ds as (select SEQ_COMMAND as integration_sales_detail_id, ds.* from _ds where not exists (select * from map where map.ec_shop_sales_detail_id = ds.ec_shop_sales_detail_id) and map.ec_shop_sales_id = ds.ec_shop_sales_id)
    /// --header sequencing
    /// , _head as (select SEQ_COMMAND as integration_sales_id from _ds group by ec_shop_sales_id)
    /// --detail inner join header
    /// , __ds as (select _head.integration_sales_id, _ds.* from _ds inner join _head on _ds.ec_shop_sales_id = _head.ec_shop_sales_id)
    /// </summary>
    string GetWithQuery();

    string BuildExtendWithQuery();

    /// <summary>
    /// ex."__ds"
    /// </summary>
    string Alias { get; }

    /// <summary>
    /// ex."tmp01"
    /// </summary>
    string BridgeName { get; }
}

public static class IBridgeExtension
{

    public static string ToSql(this IBridge source)
    {
        var ext = source.BuildExtendWithQuery();
        if (ext != null) ext = $@",
{ext}";

        var sql = $@"{source.GetWithQuery()}{ext}
create temporary table {source.BridgeName}
as
select
    *
from {source.Alias};";
        return sql;
    }
}

