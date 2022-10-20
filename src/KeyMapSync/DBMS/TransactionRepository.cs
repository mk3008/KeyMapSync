using Dapper;
using KeyMapSync.Entity;
using SqModel.Analysis;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace KeyMapSync.DBMS;

public class TransactionRepository
{
    public TransactionRepository(IDbConnection connection)
    {
        Connection = connection;
    }

    public Action<string>? Logger { get; set; } = null;

    public IDbConnection Connection { get; init; }

    public void CreateTableOrDefault()
    {
        var sql = @$"create table if not exists kms_transactions (
    kms_transaction_id serial8 not null primary key
    , destination_id int8 not null
    , datasource_id int8 not null
    , argument text not null
    , result text
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)";
        Connection.Execute(sql);
    }

    public int Insert(Datasource d, string argument)
    {
        var sql = @"insert into kms_transactions(
    destination_id
    , datasource_id
    , argument
)
values
(
    :destination_id
    , :datasource_id
    , :argument
)
returning kms_transaction_id";

        Logger?.Invoke(sql);
        return Connection.Query<int>(sql, new { destination_id = d.DestinationId, datasource_id = d.DatasourceId, argument }).First();
    }

    public int Update(int id, string result)
    {
        var sql = @"update kms_transactions
set
    result = :result
    , updated_at = clock_timestamp()
where
    kms_transaction_id = :id";

        Logger?.Invoke(sql);
        return Connection.Execute(sql, new { id, result });
    }
}
