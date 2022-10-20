using KeyMapSync.DBMS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public static class DatasourceExtension
{
    public static string GetKeymapTableName(this Datasource source, KeyMapConfig config)
    {
        if (source.HasKeymap == false) return String.Empty;
        return string.Format(config.TableNameFormat, source.Destination.TableName, source.MapName);
    }

    public static DbTable GetKeyMapDbTable(this Datasource source, KeyMapConfig config)
    {
        if (source.Destination == null) throw new InvalidProgramException();

        var name = source.GetKeymapTableName(config);

        var t = new DbTable
        {
            Table = name,
            Sequence = null,
            Primarykeys = new() { source.Destination.SequenceConfig.Column },
            UniqueKeyGroups = new() { source.KeyColumnsConfig.Select(x => x.Key).ToList() }
        };

        t.AddDbColumn(source.Destination.SequenceConfig.Column);
        source.KeyColumnsConfig.ForEach(x => t.AddDbColumn(x.Key, x.Value));

        return t;
    }
}
