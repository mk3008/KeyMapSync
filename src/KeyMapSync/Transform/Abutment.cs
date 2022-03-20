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
    public Abutment(Datasource datasource)
    {
        Datasource = datasource;
    }

    /// <summary>
    /// Datasource.
    /// </summary>
    public Datasource Datasource { get; }

    public string ViewName => $"_v_{Datasource.BridgeName}";

    public string ViewOrCteName => ViewName;

    public IPier? CurrentPier => null;

    IAbutment IBridge.Abutment =>this;
}

