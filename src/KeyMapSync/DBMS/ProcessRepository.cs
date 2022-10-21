using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class ProcessRepository
{
    public ProcessRepository(IDbConnection connection)
    {
        Connection = connection;
    }

    public Action<string>? Logger { get; set; } = null;

    public IDbConnection Connection { get; init; }

    public void CreateTableOrDefault()
    {
        var sql = @$"create table if not exists kms_processes (
    kms_process_id serial8 not null primary key
    , kms_transaction_id int8 not null
    , destination_id int8 not null
    , destination_full_name text not null
    , datasource_id int8 not null
    , datasource_name text not null
    , is_root bool not null
    , map_full_name text
    , created_at timestamp default current_timestamp
)";
        Connection.Execute(sql);
    }

    public long Insert(long tranid, Datasource d, string mapfullname)
    {
        var sql = @"insert into kms_processes(
    kms_transaction_id
    , destination_id
    , destination_full_name
    , datasource_id
    , datasource_name
    , is_root
    , map_full_name
)
values
(
    :tran_id
    , :destination_id
    , :destination_full_name
    , :datasource_id
    , :datasource_name
    , :is_root
    , :map_full_name
)
returning kms_process_id";

        Logger?.Invoke(sql);
        return Connection.Query<long>(sql, new
        {
            tran_id = tranid,
            destination_id = d.DestinationId,
            destination_full_name = d.Destination.TableFulleName,
            datasource_id = d.DatasourceId,
            datasource_name = d.DatasourceName,
            is_root = d.IsRoot,
            map_full_name = mapfullname,
        }).First();
    }
}
