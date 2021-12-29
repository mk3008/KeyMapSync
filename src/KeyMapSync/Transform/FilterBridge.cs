using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class FilterBridge : IBridge
{
    public IBridge Owner { get; set; }

    /// <summary>
    /// ex.
    /// _table_name 
    /// </summary>
    public IList<Filter> Filters { get; } = new List<Filter>();

    public string Alias => "_filtered";

    public string BridgeName => Owner.BridgeName;

    public Datasource Datasource => Owner.Datasource;

    public string BuildExtendWithQuery()
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// ex.
    /// with 
    /// _added as (
    ///    select
    ///        (select max(seq) from (select seq from sqlite_sequence where name = :_table_name union all select 0)) as integration_sale_detail_id, ds.*
    ///    from
    ///        ds
    ///    where
    ///        not exists (select * from integration_sale_detail__map_ec_shop_sale_detail _km on ds.ec_shop_sale_detail_id = _km.ec_shop_sale_detail_id)
    /// )
    /// </summary>
    /// <returns></returns>
    public string GetWithQuery()
    {
        var sql = $@"{Owner.GetWithQuery}, 
{Alias} as (
    select {Owner.Alias}.*
    from {Owner.Alias}
    {Filters.ToWhereSqlText}
)";
        return sql;
    }
}

