using KeyMapSync.Entity;
using SqModel.Analysis;
using SqModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqModel.Dapper;
using System.Text.Json;
using Dapper;

namespace KeyMapSync.DBMS;

public class SystemConfigRepository : IRepositry
{
    public SystemConfigRepository(IDBMS dbms, IDbConnection connection)
    {
        Connection = connection;
        Database = dbms;
    }

    public Action<string>? Logger { get; set; } = null;

    public IDBMS Database { get; init; }

    public IDbConnection Connection { get; init; }

    private string TableName { get; set; } = "kms_configs";

    private string IdName = $"config_name";

    private T Find<T>(Action<SelectQuery, TableClause>? filter = null)
    {
        var columns = this.GetColumns("", TableName);

        var sq = new SelectQuery();
        var t = sq.From(TableName).As("t");
        sq.Select.Add().Column(t, "config_value").As("configvalue");
        filter?.Invoke(sq, t);
        var q = sq.ToQuery();

        Logger?.Invoke(q.ToDebugString());
        var s = Connection.Query<string>(q).First();
        var c = JsonSerializer.Deserialize<T>(s);
        if (c == null) throw new Exception();
        return c;
    }

    public SystemConfig Load()
    {
        var c = new SystemConfig();
        c.KeyMapConfig = FindKeyMapConfig();
        c.SyncConfig = FindSyncConfig();
        c.OffsetConfig = FindOffsetConfig();
        c.CommandConfig = FindCommandConfig();
        c.ExtendConfig = FindExtendConfig();
        return c;
    }

    private KeyMapConfig FindKeyMapConfig()
    {
        var c = Find<KeyMapConfig>((sq, t) =>
        {
            sq.Where.Add().Column(t, IdName).Equal(":name").AddParameter(":name", "KeyMapConfig");
        });

        return c;
    }

    private SyncConfig FindSyncConfig()
    {
        var c = Find<SyncConfig>((sq, t) =>
        {
            sq.Where.Add().Column(t, IdName).Equal(":name").AddParameter(":name", "SyncConfig");
        });

        return c;
    }

    private OffsetConfig FindOffsetConfig()
    {
        var c = Find<OffsetConfig>((sq, t) =>
        {
            sq.Where.Add().Column(t, IdName).Equal(":name").AddParameter(":name", "OffsetConfig");
        });

        return c;
    }

    private ExtendConfig FindExtendConfig()
    {
        var c = Find<ExtendConfig>((sq, t) =>
        {
            sq.Where.Add().Column(t, IdName).Equal(":name").AddParameter(":name", "ExtendConfig");
        });

        return c;
    }

    private CommandConfig FindCommandConfig()
    {
        var c = Find<CommandConfig>((sq, t) =>
        {
            sq.Where.Add().Column(t, IdName).Equal(":name").AddParameter(":name", "CommandConfig");
        });

        return c;
    }

    public void CreateTableOrDefault()
    {
        var sql = @$"create table if not exists {TableName} (
    kms_config_id serial8 not null primary key
    , config_name text not null unique
    , config_value text not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)";
        Connection.Execute(sql);

        sql = @$"insert into {TableName}(
config_name, config_value
)
select
    :name
    , :value
where
    not exists(select * from kms_configs x where config_name = :name)";

        var val = JsonSerializer.Serialize(new KeyMapConfig());
        Connection.Execute(sql, new { name = "KeyMapConfig", value = val });

        val = JsonSerializer.Serialize(new SyncConfig());
        Connection.Execute(sql, new { name = "SyncConfig", value = val });

        val = JsonSerializer.Serialize(new OffsetConfig());
        Connection.Execute(sql, new { name = "OffsetConfig", value = val });

        val = JsonSerializer.Serialize(new CommandConfig());
        Connection.Execute(sql, new { name = "CommandConfig", value = val });

        val = JsonSerializer.Serialize(new ExtendConfig());
        Connection.Execute(sql, new { name = "ExtendConfig", value = val });
    }
}
