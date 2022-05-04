using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KeyMapSync.Entity;

namespace KeyMapSync.Transform;

public class InsertLoad
{
    /// <summary>
    /// ex.
    /// integration_sales_ex_ec_shop_sales_article
    /// </summary>
    public string Destination { get; set; }

    /// <summary>
    /// ex."tmp01"
    /// </summary>
    public string BridgeName { get; set; }

    /// <summary>
    /// ex.1
    /// with bridge as (select * from /*bridge*/ where ec_shop_sales_article_id is not null)
    /// 
    /// ex.2
    /// with bridge as (select * from /*bridge*/)
    /// </summary>
    public string WithQuery { get; set; }

    /// <summary>
    /// ex."bridge"
    /// </summary>
    public string AliasName { get; set; }

    public IEnumerable<string> Columns { get; set; }

    /// <summary>
    /// ex.1 detail load
    /// with bridge as (select * from tmp01)
    /// insert into integration_sale_detail(integration_sale_detail_id, integration_sales_id, price, create_timestamp)
    /// select integration_sale_detail_id, integration_sales_id, price, create_timestamp from bridge
    /// 
    /// ex.2 header load
    /// with bridge as (select * from tmp01)
    /// insert into integration_sales(integration_sales_id, sales_date)
    /// select integration_sales_id, sales_date, price from bridge
    /// 
    /// ex.3 extension load
    /// with bridge as (select * from tmp01 where ec_shop_sales_article_id is not null)
    /// insert into integration_sales_ex_ec_shop_sales_article(integration_sales_id, ec_shop_sales_article_id, create_timestamp)
    /// select integration_sales_id, ec_shop_sales_article_id, create_timestamp from bridge
    /// 
    /// ex.4 keymap load
    /// with bridge as (select * from tmp01)
    /// insert into integration_sale_detail_keymap_ec_shop_sales_detail(integration_sale_detail_id, ec_shop_sales_detail_id)
    /// select integration_sale_detail_id, ec_shop_sales_detail_id from bridge
    /// 
    /// ex.5 sync load
    /// with bridge as (select * from tmp01)
    /// insert into integration_sale_detail_sync(integration_sale_detail_id, integration_sale_detail_version_id)
    /// select integration_sale_detail_id, integration_sale_detail_version_id from bridge
    /// 
    /// ex.6 version load
    /// with v as (select :version as integration_sale_detail_version_id, :datasource_name as datasource_name, current_timestmap as create_timestamp)
    /// insert into integration_sale_detail_version(integration_sale_detail_version_id, name, create_timestamp)
    /// select integration_sale_detail_version_id, name, create_timestamp from v
    /// 
    /// </summary>
    /// <returns></returns>
    public string ToSql()
    {
        var sql = $@"{WithQuery.Replace("/*bridge*/", BridgeName)}
inesrt into {Destination} ({Columns.ToString(",")})
select ({Columns.ToString(",")}) from {AliasName}";
        return sql;
    }
}

