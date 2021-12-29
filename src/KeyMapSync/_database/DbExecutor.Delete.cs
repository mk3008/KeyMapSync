using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Database;
public partial class DbExecutor
{
    public int DeleteDestinationTableByDestinationId(SyncMap def, int destinationId)
    {
        var dest = def.DestinationTable;
        var sql = $"delete from {dest.TableFullName} where {dest.SequenceColumn.ColumnName}  = :destinationId";
        var prm = new { destinationId };

        OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
        return Connection.Execute(sql, prm);
    }

    public int DeleteMappingTableByDestinationId(SyncMap def, int destinationId)
    {
        var dest = def.DestinationTable;
        var map = def.KeyMap.MappingTable;
        var sql = $"delete from {map.TableFullName} where {dest.SequenceColumn.ColumnName} = :destinationId";
        var prm = new { destinationId };

        OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
        return Connection.Execute(sql, prm);
    }

    public int DeleteSyncTableByDestinationId(SyncMap def, int destinationId)
    {
        var dest = def.DestinationTable;
        var sync = def.KeyMap.SyncTable;
        var sql = $"delete from {sync.TableFullName} where {dest.SequenceColumn.ColumnName}  = :destinationId";
        var prm = new { destinationId };

        OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
        return Connection.Execute(sql, prm);
    }

    public int DeleteDestinationTableByVersionId(SyncMap def, int versionId)
    {
        var dest = def.DestinationTable;
        var sync = def.KeyMap.SyncTable;
        var ver = def.KeyMap.VersionTable;
        var sql = $"delete from {dest.TableFullName} where exists (select * from {sync.TableFullName} x where x.{ver.SequenceColumn.ColumnName} = :versionId and x.{dest.SequenceColumn.ColumnName} = {dest.TableFullName}.{dest.SequenceColumn.ColumnName})";
        var prm = new { versionId };

        OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
        return Connection.Execute(sql, prm);
    }

    public int DeleteMappingTableByVersionId(SyncMap def, int versionId)
    {
        var dest = def.DestinationTable;
        var map = def.KeyMap.MappingTable;
        var sync = def.KeyMap.SyncTable;
        var ver = def.KeyMap.VersionTable;
        var sql = $"delete from {map.TableFullName} where exists (select * from {sync.TableFullName} x where x.{ver.SequenceColumn.ColumnName} = :versionId and x.{dest.SequenceColumn.ColumnName} = {map.TableFullName}.{dest.SequenceColumn.ColumnName})";
        var prm = new { versionId };

        OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
        return Connection.Execute(sql, prm);
    }

    public int DeleteSyncTableByVersionId(SyncMap def, int versionId)
    {
        var sync = def.KeyMap.SyncTable;
        var ver = def.KeyMap.VersionTable;
        var sql = $"delete from {sync.TableFullName} where {ver.SequenceColumn.ColumnName} = :versionId";
        var prm = new { versionId };

        OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, prm));
        return Connection.Execute(sql, prm);
    }

    public int OffsetMapping(SyncMap def)
    {
        var idName = def.DestinationTable.SequenceColumn.ColumnName;
        var sql = $"delete from {def.KeyMap.MappingTable.TableFullName} where exists (select * from {def.DestinationTable.TableName}_map_offset x where x.offset_{idName} = {def.KeyMap.MappingTable.TableFullName}.{idName})";

        OnBeforeExecute?.Invoke(this, new SqlEventArgs(sql, null));
        return Connection.Execute(sql);
    }
}