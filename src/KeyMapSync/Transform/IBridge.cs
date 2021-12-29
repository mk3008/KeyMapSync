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

        /// <summary>
        /// ex.
        /// with ds as (select * from ec_shop_sales_detail)
        /// --unsync filter and sequencing
        /// , _ds as (select SEQ_COMMAND as integration_sales_detail_id, ds.* from _ds where not exists (select * from map where map.ec_shop_sales_detail_id = ds.ec_shop_sales_detail_id) and map.ec_shop_sales_id = ds.ec_shop_sales_id)
        /// --header sequencing
        /// , _head as (select SEQ_COMMAND as integration_sales_id from _ds group by ec_shop_sales_id)
        /// --detail inner join header
        /// , __ds as (select _head.integration_sales_id, _ds.* from _ds inner join _head on _ds.ec_shop_sales_id = _head.ec_shop_sales_id)
        /// create table tmp01 as select * from __ds
        /// 
        /// ex.offset
        /// with ds as (select * from ec_shop_detail)
        /// --actual
        /// , _now_values as (select * from ds where exists (select * from map where map.ec_shop_sales_detail_id = ds.ec_shop_sales_detail_id) 
        /// --expect
        /// , _sync_values as (select h.ec_shop_sales_id, d.ec_shop_sales_detail_id, h.sales_date, d.price from integration_sales_detail d inner join integration_sales h on d.integration_sales_id = h.integration_sales_id)
        /// --validate
        /// , _ds as (select s.ec_shop_sales_id, s.ec_shop_sales_detail_id, s.sales_date, s.price * -1 as price from _sync_values s left join _now_values n where n is null or s <> n)
        /// create table tmp01 as select * from _ds
        /// </summary>
        /// <returns></returns>

        var sql = $@"{source.GetWithQuery()}
create table {source.BridgeName}
as
select * from {source.Alias};";
        return sql;
    }
}

