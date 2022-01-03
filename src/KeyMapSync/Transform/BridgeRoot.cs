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

    public string DatasourceView => $"_kms_v_{Datasource.Name}";

    /// <summary>
    /// ex."ds"
    /// </summary>
    public string Alias => DatasourceView;

    /// <summary>
    /// ex."tmp01"
    /// </summary>
    public string BridgeName { get; set; }

    public IFilter Filter => null;

    public string BuildExtendWithQuery()
    {
        return null;
    }

    public BridgeRoot GetRoot() => this;

    public string GetWithQuery() => null;

    public string ToTemporaryViewDdl()
    {
        var sql = $@"create temporary view {DatasourceView}
as
{Datasource.Query}";
        return sql;
    }
}

