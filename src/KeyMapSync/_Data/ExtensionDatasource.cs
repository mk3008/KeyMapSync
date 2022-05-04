using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Data;

/// <summary>
/// extension datasource
/// </summary>
public class ExtensionDatasource : IDatasource
{
    public string DatasourceName { get; set; }

    public string DestinaionTableName { get; set; }

    /// <summary>
    /// ex. "with _ds as (select * from /*Bridge*/)"
    /// </summary>
    public string WithQueryText { get; set; }

    /// <summary>
    /// ex. _ds
    /// </summary>
    public string AliasName { get; set; } = "_ds";

    public ParameterSet ParameterSet { get; set; }

    /// <summary>
    /// mapping name.
    /// but extension datasource not has mapping table.
    /// </summary>
    public string MappingName => null;

    /// <summary>
    /// datasource key columns.
    /// but extension datasource not has key.
    /// </summary>
    public IEnumerable<string> DatasourceKeyColumns => Enumerable.Empty<string>();

    /// <summary>
    /// extension datasource.
    /// but extension datasource not has extension datasource.
    /// </summary>
    public IList<ExtensionDatasource> Extensions => Enumerable.Empty<ExtensionDatasource>().ToList();
}

