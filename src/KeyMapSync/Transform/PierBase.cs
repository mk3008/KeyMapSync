using KeyMapSync.DBMS;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public abstract class PierBase : IPier
{
    public PierBase(IBridge bridge)
    {
        PreviousBridge = bridge;
        if (bridge is IPier) PreviousPrier = (IPier)bridge;
    }

    public abstract string CteName { get; }

    protected IBridge PreviousBridge { get; }

    public IPier? PreviousPrier { get; }

    public FilterContainer Filter { get; } = new FilterContainer();

    public string ViewOrCteName => CteName;

    public IAbutment Abutment => PreviousBridge.Abutment;

    public IPier? CurrentPier => this;

    public abstract string AliasName { get; }

    public abstract string ToSelectQuery();

    public abstract SelectTable ToSelectTable();
}