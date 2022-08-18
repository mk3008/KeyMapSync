using KeyMapSync.DBMS;
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
    IAbutment Abutment { get; }

    IPier? CurrentPier { get; }

    string ViewOrCteName { get; }
}

public interface IAbutment : IBridge
{
    Datasource Datasource { get; }

    public string ViewName { get; }

    BridgeCommand BridgeCommand { get; }
}

public interface IPier : IBridge
{
    string AliasName { get; }

    IPier? PreviousPrier { get; }

    string ToSelectQuery();

    SelectTable ToSelectTable();

    FilterContainer Filter { get; }

    string CteName { get; }
}

