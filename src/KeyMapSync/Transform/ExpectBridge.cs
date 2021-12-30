using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

/// <summary>
/// TODO
/// </summary>
public class ExpectBridge : IBridge
{
    public IBridge Owner { get; set; }

    public Datasource Datasource => Owner.Datasource;

    public string Alias => "_expect";

    public FilterContainer FilterContainer { get; } = new FilterContainer();

    public IFilter Filter => FilterContainer;

    public string BridgeName => Owner.BridgeName;

    /// <summary>
    /// ex.
    /// _expect as (
    ///    select
    ///        _km.ec_shop_sale_detail_id, _origin.*
    ///    from
    ///        integration_sale_detail _origin
    ///        inner join integration_sale_detail__map_ec_shop_sale_detail _km on from.integration_sale_detail_id = _km.integration_sale_detail_id
    ///    where
    ///        exists (select * from integration_sale_detail__sync _sync where _sync.version_id between :_min_version_id and :_max_version_id and _origin.integration_sale_detail = _sync.integration_sale_detail)
    ///),
    /// </summary>
    /// <returns></returns>
    public string GetWithQuery()
    {
        var sql = $@"{Owner.GetWithQuery()},
{BuildExtendWithQuery()}";
        return sql;
    }

    public string BuildExtendWithQuery()
    {
        var ds = Owner.Datasource;
        var datasourceKeys = ds.KeyColumns;
        var dest = ds.Destination;
        var keymap = ds.KeyMapName;

        var cols = datasourceKeys.Select(x => $"__map.{x}").ToList();
        cols.Add($"{this.GetInnerDatasourceAlias()}.*"); 
        var col = cols.ToString("\r\n, ").AddIndent(4);

        var sql = $@"select
{col}
from {dest.DestinationName} {this.GetInnerDatasourceAlias()}
inner join {keymap} __map on {this.GetInnerDatasourceAlias()}.{dest.SequenceKeyColumn} = __map.{dest.SequenceKeyColumn}
{Filter.ToCondition(this).ToWhereSqlText()}";

        sql = $@"{Alias} as (
{sql.AddIndent(4)}
)";
        return sql;
    }
}

