using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utf8Json;

namespace KeyMapSync.DBMS;

public partial class Postgres
{

    public Destination CreateDestination(string table, string? schema = null)
    {
        var prm = GetTable(schema, table);

        var sql = @"select
    column_name
from
    information_schema.columns
where
    table_schema = :schema
    and table_name = :table
order by
    ordinal_position";

        var d = new Destination() { SchemaName = prm.schema, TableName = prm.table };
        d.Columns = Connection.Query<string>(sql, new { prm.schema, prm.table }).ToList();
        d.Sequence = GetSequence(prm.schema, prm.table);
        d.HasKeyMap = true;
        d.UseVersioning = true;
        d.AllowOffset = true;

        var creator = new DestinationSystemTableCreator(Connection, this, d);
        creator.Execute();

        var tbl = d.GetVersionTableName();
        if (tbl != null) d.VersioningConfig = new VersioningConfig() { Sequence = GetSequence(prm.schema, tbl) };

        return d;
    }

    public void RegistDestination(Destination d)
    {
        var json = JsonSerializer.Serialize(d);

        var c = ResolveDestinationOrDefault(d.DestinationName);
        if (c == null)
        {
            var sql = @$"insert into {DestinationManagementTable} (destination_name, config)
select
    :name as destination_name
    , :config::json as config";

            Connection.Execute(sql, new { name = d.DestinationName, config = json });
        }
        else
        {
            var sql = @$"update {DestinationManagementTable}
set
    config = :config::json
where
    destination_name = :name";

            Connection.Execute(sql, new { name = d.DestinationName, config = json });
        }
    }

    public Destination ResolveDestinationOrDefault(string name, Func<Destination>? creator = null)
    {
        var sql = $@"select
    config
from
    {DestinationManagementTable}
where
    destination_name = :name";

        var json = Connection.Query<string>(sql, new { name }).FirstOrDefault();
        if (json == null)
        {
            if (creator != null)
            {
                var x = creator();
                RegistDestination(x);
                return x;
            }
            throw new Exception($"destination is not found.(name : {name})");
        }

        var d = JsonSerializer.Deserialize<Destination>(json);
        if (d == null) throw new Exception($"destination deserialize is fail.(name : {name})");

        return d;
    }

    public Destination ResolveDestination(string name)
        => ResolveDestinationOrDefault(name);
}
