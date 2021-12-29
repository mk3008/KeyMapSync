using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class Datasource
{
    /// <summary>
    /// id.
    /// </summary>
    public int? DatasourceId { get; set; }

    /// <summary>
    /// ex."ec_shop_sales_detail"
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// datasource description.
    /// ex."transfrom 'ec_shop_sales_detail' to 'integration_sales_detail'." 
    /// </summary>
    public string Description { get; set; }

    /// <summary>
    /// ex."with ds as (select * from ec_shop_sales_detail)"
    /// </summary>
    public string WithQuery { get; set; }

    /// <summary>
    /// ex."ds"
    /// </summary>
    public string Alias { get; set; }

    /// <summary>
    /// ex.
    /// {
    /// "DestinationName":"integration_sales_detail"
    /// "SequenceKeyColumn":"integration_sales_detail_id",
    /// "SequenceCommand":"(select max(seq) from (select seq from sqlite_sequence where name = 'integration_sales_detail' union all select 0))",
    /// }
    /// </summary>
    public Destination Destination { get; set; }

    /// <summary>
    /// datasource key column names.
    /// ex.
    /// "ec_shop_sales_id, ex_shop_sales_dtail_id"
    /// </summary>
    public IList<string> KeyColumns { get; set; }

    /// <summary>
    /// ex.
    /// "sales_date, article_name, unit_price, quantity, price, sync_timestamp"
    /// </summary>
    public IList<string> Columns { get; set; }

    /// <summary>
    /// ex
    /// "sync_timestamp"
    /// </summary>
    public IList<string> InspectionIgnoreColumns { get; set; }

    /// <summary>
    /// ex
    /// "quantity, price"
    /// </summary>
    public IList<string> SingInversionColumns { get; set; }

    public IEnumerable<ExtensionDatasource> Extensions { get; set; }

    public HeaderOption HeaderOption { get; set; }

    public string KeyMapFormat { get; set; } = "{0}__map_{1}";

    public string SyncFormart { get; set; } = "{0}__sync";

    public string VersionFormat { get; set; } = "{0}__version";

    /// <summary>
    /// ex
    /// "integration_sales_detail__map_ec_shop_sales_detail"
    /// </summary>
    public string KeyMapName => string.Format(KeyMapFormat, Destination.DestinationName, Name);

    /// <summary>
    /// ex
    /// "integration_sales_detail__sync"
    /// </summary>
    public string SyncName => string.Format(SyncFormart, Destination.DestinationName, Name);

    /// <summary>
    /// ex
    /// "integration_sales_detail__version"
    /// </summary>
    public string VersionName => string.Format(VersionFormat, Destination.DestinationName, Name);
}


