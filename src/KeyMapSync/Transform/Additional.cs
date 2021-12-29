using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class Additional : IBridge
{
    public IBridge Owner { get; set; }

    public Datasource Datasource => Owner.Datasource;

    public IAdditionalCondition AdditionalCondition { get; set; }

    public string Alias => "_added";

    public string BridgeName => Owner.BridgeName;

    /// <summary>
    /// ex.
    /// with 
    /// ds as (...)
    /// _added as (
    ///    select
    ///        sequnce_command as integration_sale_detail_id, ds.*
    ///    from
    ///        ds
    ///    where
    ///        not exists (select * from map _km on ds.ec_shop_sale_detail_id = _km.ec_shop_sale_detail_id)
    /// )
    /// </summary>
    /// <returns></returns>
    public string GetWithQuery()
    {
        var dst = Datasource.Destination;

        var sql = $@"{Owner.GetWithQuery()},
{Alias} as (
    select {dst.SequenceCommand} as {dst.SequenceKeyColumn}, {Owner.Alias}.*
    from {Owner.Alias}
    {AdditionalCondition.ToFilter(this).ToWhereSqlText()}
)";
        return sql;
    }
}

