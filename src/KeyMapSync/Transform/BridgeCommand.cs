using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class BridgeCommand
{
    public CommandTypes CommandType => (ValidateFilter != null) ? CommandTypes.Offset : CommandTypes.Insert;

    public Datasource? Datasource { get; set; } = null;

    public IFilter? ValidateFilter { get; set; } = null;

    public IFilter? Filter { get; set; } = null;

    public IPier BuildBridge()
    {
        if (Datasource == null) throw new InvalidOperationException();

        //create bridge instance.
        if (ValidateFilter != null)
        {
            var root = new Abutment(Datasource, this);
            var pier = new ExpectPier(root, ValidateFilter);
            if (Filter != null) pier.AddFilter(Filter);
            return new ChangedPier(pier);
        }
        else
        {
            var root = new Abutment(Datasource, this);
            var bridge = new AdditionalPier(root);
            if (Filter != null) bridge.AddFilter(Filter);
            return bridge;
        }
    }

    public List<IPier> BuildExtensionBridges(Datasource parent)
    {
        var bridges = new List<IPier>();

        if (Datasource == null) throw new InvalidOperationException();
    
        //create bridge instance.
        if (ValidateFilter != null)
        {
            foreach (var item in parent.OffsetExtensions)
            {
                var abutment = new Abutment(item, this);
                var bridge = new ExtensionAdditionalPier(abutment);
                bridges.Add(bridge);
            }
            return bridges;
        }
        else
        {
            foreach (var item in parent.Extensions)
            {
                var abutment = new Abutment(item, this);
                var bridge = new ExtensionAdditionalPier(abutment);
                bridges.Add(bridge);
            }
            return bridges;
        }
    }

    public enum CommandTypes
    {
        Insert = 0,
        Offset = 1,
    }
}
