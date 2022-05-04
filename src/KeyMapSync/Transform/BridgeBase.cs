using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public abstract class BridgeBase : IBridge
{
    public abstract string Name { get; }

    public abstract IAbutment GetAbutment();

    public abstract IPier GetCurrentPier();

  
}

