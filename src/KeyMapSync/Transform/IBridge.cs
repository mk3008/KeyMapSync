using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public interface IBridge
{
    IAbutment GetAbutment();

    IPier? GetCurrentPier();

    string Name { get; }
}

public interface IAbutment : IBridge
{
    Datasource Datasource { get; }

    /// <summary>
    /// ex."tmp01"
    /// </summary>
    string BridgeName { get; }
}

public interface IPier : IBridge
{

    IPier? PreviousPrier { get; }

    string BuildCurrentSelectQuery();

    FilterContainer Filter { get; }
}

