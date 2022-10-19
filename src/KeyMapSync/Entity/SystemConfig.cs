using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class SystemConfig
{
    public KeyMapConfig KeyMapConfig { get; set; } = new();

    public SyncConfig SyncConfig { get; set; } = new();

    public OffsetConfig OffsetConfig { get; set; } = new();

    public CommandConfig CommandConfig { get; set; } = new();
}

//public class ConfigTable
//{
//    public string SchemaName { get; set; } = string.Empty;

//    public string TableName { get; set; } = string.Empty;

//    public Sequence Sequence { get; set; } = new();

//    public string TableFullName => GetTableFullName();

//    private string GetTableFullName()
//    {
//        if (String.IsNullOrEmpty(SchemaName)) return TableName;
//        return $"{SchemaName}.{TableName}";
//    }
//}

//public class TransactionConfig : ConfigTable
//{
//    public string ArgumentColumn { get; set; } = "argument";
//}

//public class ProcessConfig : ConfigTable
//{
//    public string MapColumn { get; set; } = "mapping_name";
//}

//public class DestinationConfig : ConfigTable
//{
//    public string NameColumn { get; set; } = "destination_name";

//    public string ConfigColumn { get; set; } = "config";
//}

//public class DatasourceConfig : ConfigTable
//{
//    public string NameColumn { get; set; } = "datasource_name";

//    public string MapColumn { get; set; } = "mapping_name";

//    public string GroupColumn { get; set; } = "group_name";

//    public string ConfigColumn { get; set; } = "config";

//    public string DisableColumn { get; set; } = "disable";
//}