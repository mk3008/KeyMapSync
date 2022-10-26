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
using Dapper;
using SqModel.Extension;
using System.Text.Json;

namespace KeyMapSync.DBMS;

public class DestinationRepository : IRepositry
{
    public DestinationRepository(IDBMS dbms, IDbConnection connection)
    {
        Connection = connection;
        Database = dbms;

    }

    public Action<string>? Logger { get; set; } = null;

    public IDBMS Database { get; init; }

    public IDbConnection Connection { get; init; }

    private string TableName { get; set; } = "kms_destinations";

    private string IdColumn = $"destination_id";

    private string SchemaNameColumn = $"schema_name";

    private string TableNameColumn = $"table_name";

    public List<Destination> Find(Action<SelectQuery, TableClause>? filter = null)
    {
        var columns = this.GetColumns("", TableName);

        var sql = @"select
    d.destination_id
    , d.base_destination_id
    , d.description
    , d.schema_name
    , d.table_name
    , d.sequence_config
    , d.columns
    , d.allow_offset
    , h.key_columns
    , h.query
    , od.sign_inversion_columns
    , od.inspection_ignore_columns
from
    kms_destinations d
    left join kms_header_destinations h on d.destination_id = h.destination_id
    left join kms_offsettable_destinations od on d.destination_id = od.destination_id and h.destination_id is null";

        var sq = SqlParser.Parse(sql);

        var t = sq.FromClause;
        sq.GetSelectItems().ForEach(x => x.Name = x.Name.Replace("_", ""));
        filter?.Invoke(sq, t);
        var q = sq.ToQuery();

        Logger?.Invoke(q.ToDebugString());

        SqlMapper.AddTypeHandler(new SequenceTypeHandler());
        var lst = Connection.Query<Destination>(q).ToList();
        SqlMapper.ResetTypeHandlers();

        lst.ForEach(x =>
        {
            x.HeaderDestination = FindByBaseId(x.DestinationId);
        });

        return lst;
    }

    public Destination FindById(long id)
    {
        var lst = Find((sq, t) =>
        {
            sq.Where.Add().Column(t, IdColumn).Equal(":id").AddParameter(":id", id);
        });

        return lst.First();
    }

    public Destination? FindByBaseId(long id)
    {
        var lst = Find((sq, t) =>
        {
            sq.Where.Add().Column(t, "base_destination_id").Equal(":id").AddParameter(":id", id);
        });

        return lst.FirstOrDefault();
    }

    public Destination? FindByName(string schema, string table)
    {
        var lst = Find((sq, t) =>
        {
            if (!string.IsNullOrEmpty(schema)) sq.Where.Add().Column(t, SchemaNameColumn).Equal(":schema").AddParameter(":schema", schema);
            sq.Where.Add().Column(t, TableNameColumn).Equal(":table").AddParameter(":table", table);
        });
        if (lst.Count > 1) throw new Exception();
        return lst.FirstOrDefault();
    }

    public Destination Save(string schema, string table)
    {
        var d = new Destination() { SchemaName = schema, TableName = table, AllowOffset = false };
        d.Columns = this.GetColumns(schema, table).ToArray();
        d.SequenceConfig = this.GetSequence(schema, table);

        Save(d);
        return d;
    }

    public Destination SaveAsHeader(string schema, string table, string[] keycolumns, string query, string baseschema, string basetable)
    {
        var based = FindByName(baseschema, basetable);
        if (based == null) throw new Exception($"Base destination is not found.(schema : {baseschema}, table : {basetable})");

        var d = new Destination() { SchemaName = schema, TableName = table, KeyColumns = keycolumns, AllowOffset = false, Query = query };
        d.BaseDestinationId = based.DestinationId;
        d.Columns = this.GetColumns(schema, table).ToArray();
        d.SequenceConfig = this.GetSequence(schema, table);

        Save(d);
        return d;
    }

    public void Save(Destination d)
    {
        var dbcolumns = this.GetColumns("", TableName);

        var sq = SqlParser.Parse(d, nameconverter: x => x.ToSnakeCase().ToLower());
        //var config = JsonSerializer.Serialize(d.SequenceConfig);
        //sq.Select.Add().Value(":seq").As("sequence_config").AddParameter(":seq", config);

        sq.RemoveSelectItem(dbcolumns);

        var dbdata = FindByName(d.SchemaName, d.TableName);

        if (dbdata != null && d.DestinationId != dbdata.DestinationId)
        {
            throw new InvalidOperationException($"This name is already exists.(schema : {d.SchemaName}, table : {d.TableName})");
        }

        SqlMapper.AddTypeHandler(new SequenceTypeHandler());
        if (dbdata == null)
        {
            var q = sq.ToInsertQuery(TableName, new() { IdColumn });
            q.CommandText += $"\r\nreturning {IdColumn}";
            Logger?.Invoke(q.ToDebugString());
            d.DestinationId = Connection.Query<int>(q).First();
        }
        else
        {
            sq.Select.Add().Value("clock_timestamp()").As("updated_at");
            var q = sq.ToUpdateQuery(TableName, new() { IdColumn });
            Logger?.Invoke(q.ToDebugString());
            Connection.Execute(q);
        }
        SqlMapper.ResetTypeHandlers();

        if (d.BaseDestinationId != null && !string.IsNullOrEmpty(d.Query) && d.KeyColumns.Any())
        {
            var hq = SqlParser.Parse(d.Query);
            SaveExtension(d, "kms_header_destinations");
            DeleteExtension(d, "kms_offsettable_destinations");
        }
        else if (d.SignInversionColumns.Any())
        {
            DeleteExtension(d, "kms_header_destinations");
            SaveExtension(d, "kms_offsettable_destinations");
        }
        else
        {
            DeleteExtension(d, "kms_header_destinations");
            DeleteExtension(d, "kms_offsettable_destinations");
        }
    }

    private void SaveExtension(Destination d, string extable)
    {
        var dbcolumns = this.GetColumns("", extable);

        var sq = SqlParser.Parse(d, nameconverter: x => x.ToSnakeCase().ToLower());
        sq.RemoveSelectItem(dbcolumns);

        var cnt = Connection.ExecuteScalar<int>($"select count(*) from {extable} where destination_id = :id", new { id = d.DestinationId });

        SqlMapper.AddTypeHandler(new DictionaryTypeHandler());
        if (cnt == 0)
        {
            var q = sq.ToInsertQuery(extable, new());
            Logger?.Invoke(q.ToDebugString());
            Connection.Execute(q);
        }
        else
        {
            sq.Select.Add().Value("clock_timestamp()").As("updated_at");
            var q = sq.ToUpdateQuery(extable, new() { IdColumn });
            Logger?.Invoke(q.ToDebugString());
            Connection.Execute(q);
        }
        SqlMapper.ResetTypeHandlers();
    }

    private void DeleteExtension(Destination d, string table)
    {
        Connection.Execute($"delete from {table} where destination_id = :id", new { id = d.DestinationId });
    }

    public void CreateTableOrDefault()
    {
        var sql = @$"
create table if not exists kms_destinations (
    destination_id serial8 not null primary key
    , base_destination_id int8 unique
    , description text not null
    , schema_name text not null
    , table_name text not null
    , sequence_config text not null
    , columns text[] not null
    , allow_offset bool not null default false
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
    , unique(schema_name, table_name)
    , check(case when base_destination_id is not null and allow_offset = true then false else true end)
)
;
create table if not exists kms_offsettable_destinations (
    destination_id int8 not null primary key
    , sign_inversion_columns text[] not null
    , inspection_ignore_columns text[] not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)
;
create table if not exists kms_header_destinations (
    destination_id int8 not null primary key
    , key_columns text[] not null
    , query text not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)
";
        Connection.Execute(sql);
    }
}
