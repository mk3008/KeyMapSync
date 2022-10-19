﻿using Dapper;
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

    public IDbConnection Connection { get; init; }

    public void CreateTableOrDefault()
    {
        var sql = @$"create table if not exists kms_processes (
    kms_process_id serial8 not null primary key
    , kms_transaction_id int8 not null
    , destination_id int8 not null
    , destination_full_name text not null
    , datasource_id int8 not null
    , datasource_full_name text not null
    , map_full_name text
    , created_at timestamp default current_timestamp
)";
        Connection.Execute(sql);
    }

    public int Insert(int tranid, Datasource d, string mapfullname)
    {
        var sql = @"insert into kms_processes(
    kms_transaction_id
    , destination_id
    , destination_full_name
    , datasource_id
    , datasource_full_name
    , map_full_name text
)
values
(
    :tran_id
    , :destination_id
    , :destination_full_name
    , :datasource_id
    , :datasource_full_name
    , :map_full_name text
)
returning kms_transactions_id";

        return Connection.Execute(sql, new
        {
            tran_id = tranid,
            destination_id = d.DestinationId,
            destination_full_name = d.Destination.TableFulleName,
            datasource_id = d.DatasourceId,
            datasource_full_name = d.TableFulleName,
            map_full_name = mapfullname,
        });
    }
}