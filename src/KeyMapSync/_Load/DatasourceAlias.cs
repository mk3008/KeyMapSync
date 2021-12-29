using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KeyMapSync.Data;

namespace KeyMapSync.Load;

public class DatasourceAlias
{
    public IDatasource Datasource { get; set; }

    /// <summary>
    /// ex."with datasource as (select * table_name)"
    /// </summary>
    public string WithQueryText => Datasource.WithQueryText;

    /// <summary>
    /// ex."_ds"
    /// </summary>
    public string AliasName => Datasource.AliasName;

    public IEnumerable<string> KeyColumns => Datasource.DatasourceKeyColumns;

    /// <summary>
    /// ex."shop_id = :shop_id"
    /// </summary>
    public ParameterSet ParameterSet => Datasource.ParameterSet;
}
