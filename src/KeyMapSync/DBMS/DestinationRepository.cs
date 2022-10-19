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

        var sq = new SelectQuery();
        var t = sq.From(TableName).As("t");
        columns.ForEach(x => sq.Select.Add().Column(t, x).As(x.Replace("_", "")));
        filter?.Invoke(sq, t);
        var q = sq.ToQuery();

        Logger?.Invoke(q.ToDebugString());

        SqlMapper.AddTypeHandler(new SequenceTypeHandler());
        var lst = Connection.Query<Destination>(q).ToList();
        SqlMapper.ResetTypeHandlers();

        return lst;
    }

    public Destination FindById(int id)
    {
        var lst = Find((sq, t) =>
        {
            sq.Where.Add().Column(t, IdColumn).Equal(":id").AddParameter(":id", id);
        });

        return lst.First();
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

    public Destination GetScaffold(string schema, string table)
    {
        var d = new Destination() { SchemaName = schema, TableName = table, AllowOffset = true };
        d.Columns = this.GetColumns(schema, table).ToArray();
        d.SequenceConfig = this.GetSequence(schema, table);
        return d;
    }

    public void Save(Destination d)
    {
        var dbcolumns = this.GetColumns("", TableName);

        var sq = SqlParser.Parse(d, nameconverter: x => x.ToSnakeCase().ToLower());
        //var config = JsonSerializer.Serialize(d.SequenceConfig);
        //sq.Select.Add().Value(":seq").As("sequence_config").AddParameter(":seq", config);

        sq.RemoveSelectItem(dbcolumns);

        SqlMapper.AddTypeHandler(new SequenceTypeHandler());
        if (d.DestinationId == 0)
        {
            var q = sq.ToInsertQuery(TableName, new() { IdColumn });
            q.CommandText += $"\r\nreturning {IdColumn}";
            Logger?.Invoke(q.ToDebugString());
            d.DestinationId = Connection.Query<int>(q).First();
        }
        else
        {
            sq.Select.Add().Value("current_timestamp").As("updated_at");
            var q = sq.ToUpdateQuery(TableName, new() { IdColumn });
            Logger?.Invoke(q.ToDebugString());
            Connection.Execute(q);
        }
        SqlMapper.ResetTypeHandlers();
    }

    public void CreateTableOrDefault()
    {
        var sql = @$"create table if not exists {TableName} (
    destination_id serial8 not null primary key
    , description text not null
    , schema_name text not null
    , table_name text not null
    , sequence_config text not null
    , columns text[] not null
    , allow_offset bool not null default true
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
    , unique(schema_name, table_name)
)";
        Connection.Execute(sql);
    }
}
