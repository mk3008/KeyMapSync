using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class Abutment : IAbutment
{
    public Abutment(Datasource datasource, string bridgeName = "")
    {
        Datasource = datasource;
        BridgeName = string.IsNullOrEmpty(bridgeName) ? string.Format(BridgeNameFormat, Datasource.Name) : bridgeName;
    }

    /// <summary>
    /// Datasource.
    /// </summary>
    public Datasource Datasource { get; }

    public string ViewNameFormat => "_kms_v_{0}";

    /// <summary>
    /// Name of the view wrapping the data source.
    /// ex."_kms_v_datasource"
    /// </summary>
    public string Name => String.Format(ViewNameFormat, Datasource.Name);

    public string BridgeNameFormat => "_kms_tmp_{0}";

    /// <summary>
    /// Name of the temporary table that will eventually be created.
    /// ex."tmp01"
    /// </summary>
    public string BridgeName { get; }

    public IAbutment GetAbutment() => this;

    public IPier? GetCurrentPier() => null;
}

