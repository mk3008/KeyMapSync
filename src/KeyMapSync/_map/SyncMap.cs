using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

    /// <summary>
    /// Manages mapping information used for <code>Synchronizer</code>
    /// </summary>
public class SyncMap
{
    public SyncMap Sender { get; set; }

    public KeyMap KeyMap { get; set; }

    public Table DestinationTable { get; set; }

    [Obsolete("use BridgeTableName")]
    public DatasourceTable DatasourceTable => new DatasourceTable() { TableName = BridgeTableName };

    public string BridgeTableName { get; set; }

    public IDatasourceMap DatasourceMap { get; set; }

    public string DatasourceName { get; set; }

    /// <summary>
    /// For Offset
    /// </summary>
    public SyncMap Origin { get; set; }

    /// <summary>
    /// For offset.
    /// If all items are deleted, the table will be created after the expected value becomes zero.
    /// In the case of normal synchronization,
    /// if Bridge is empty, there is no need for subsequent processing,
    /// but in the case of Offset, deletion processing is required,
    /// so subsequent processing is required.
    /// </summary>
    public bool MustCascade { get; set; } = false;

    public IList<SyncMap> Cascades { get; } = new List<SyncMap>();

    public string GetSummary(int nest = 0)
    {
        var s = GetSummaryCore(nest);

        foreach (var item in Cascades)
        {
            var level = nest + 1;
            s += item.GetSummary(level);
        }

        return s;
    }

    private string GetSummaryCore(int nest)
    {
        var datasource = (Sender == null) ? DatasourceName : Sender.BridgeTableName;

        var s = new StringBuilder();
        var indent = Space(nest * 3);

        if (nest == 0) s.AppendLine($"datasource:{datasource}");

        if (DatasourceMap.IsExtension)
        {
            s.AppendLine($"{indent}-> destination:{DestinationTable.TableName}");
            return s.ToString();
        }

        if (DatasourceMap.IsBridge)
        {
            s.AppendLine($"{indent}-> bridge:{BridgeTableName}");
            return s.ToString();
        }

        if (KeyMap == null)
        {
            s.AppendLine($"{indent}-> bridge:{BridgeTableName}");
            s.AppendLine($"{indent}   -> destination:{DestinationTable.TableName}");
            return s.ToString();
        }

        s.AppendLine($"{indent}-> bridge:{BridgeTableName}");
        s.AppendLine($"{indent}   -> destination:{DestinationTable.TableName}");
        s.AppendLine($"{indent}   -> sync:{KeyMap.SyncTable.TableName}");
        s.AppendLine($"{indent}   -> syncversion:{KeyMap.VersionTable.TableName}");
        s.AppendLine($"{indent}   -> map:{KeyMap.MappingTable.TableName}");
        return s.ToString();
    }

    private string Space(int length)
    {
        var s = string.Empty;
        for (int i = 0; i < length; i++)
        {
            s += " ";
        }
        return s;
    }
}
