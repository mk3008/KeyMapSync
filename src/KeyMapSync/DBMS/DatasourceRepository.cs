using Dapper;
using KeyMapSync.Entity;
using SqModel;
using SqModel.Analysis;
using SqModel.Dapper;
using SqModel.Expression;
using SqModel.Extension;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.XPath;
using Utf8Json;

namespace KeyMapSync.DBMS;

public class DatasourceRepository : IRepositry
{
    public DatasourceRepository(IDBMS dbms, IDbConnection connection)
    {
        Connection = connection;
        Database = dbms;
    }

    public Action<string>? Logger { get; set; } = null;

    public IDBMS Database { get; init; }

    public IDbConnection Connection { get; init; }

    private string TableName { get; set; } = "kms_datasources";

    private string IdColumn = $"datasource_id";

    private string DatasourceNameColumn = $"datasource_name";

    public List<Datasource> Find(Action<SelectQuery, TableClause>? filter = null)
    {
        var columns = this.GetColumns("", TableName);

        var sq = new SelectQuery();
        var t = sq.From(TableName).As("t");
        columns.ForEach(x => sq.Select.Add().Column(t, x).As(x.Replace("_", "")));
        filter?.Invoke(sq, t);
        var q = sq.ToQuery();

        Logger?.Invoke(q.ToDebugString());

        SqlMapper.AddTypeHandler(new DictionaryTypeHandler());
        var lst = Connection.Query<Datasource>(q).ToList();
        SqlMapper.ResetTypeHandlers();

        var rep = new DestinationRepository(Database, Connection) { Logger = Logger };
        lst.ForEach(x =>
        {
            x.Destination = rep.FindById(x.DestinationId);
            x.Extensions = FindByParentId(x.DatasourceId);
        });
        return lst;
    }

    public Datasource FindById(int id)
    {
        var lst = Find((sq, t) =>
        {
            sq.Where.Add().Column(t, IdColumn).Equal(":id").AddParameter(":id", id);
        });

        return lst.First();
    }

    public Datasource? FindByName(string name, string destschame, string desttable)
    {
        var lst = Find((sq, t) =>
        {
            sq.Where.Add().Column(t, DatasourceNameColumn).Equal(":name").AddParameter(":name", name);
            sq.Where.Add().Exists(xq =>
            {
                var x = xq.From("kms_destinations").As("x");
                xq.SelectAll();
                xq.Where.Add().Column(x, "destination_id").Equal(t, "destination_id");
                xq.Where.Add().Column(x, "schema_name").Equal(":schema").AddParameter(":schema", destschame);
                xq.Where.Add().Column(x, "table_name").Equal(":table").AddParameter(":table", desttable);
            });
            return;
        });
        if (lst.Count > 1) throw new Exception();
        return lst.FirstOrDefault();
    }

    public List<Datasource> FindByParentId(int parentid)
    {
        var lst = Find((sq, t) =>
        {
            sq.Where.Add().Column(t, "parent_datasource_id").Equal(":id").AddParameter(":id", parentid);
        });
        return lst;
    }

    public List<Datasource> FindByGroup(string group)
    {
        var lst = Find((sq, t) =>
        {
            sq.Where.Add().Column(t, "group_name").Equal(":group").AddParameter(":group", group);
        });
        return lst;
    }

    public Datasource GetScaffold(string name, string schema, string table, string query, string destschema, string desttable)
    {
        var d = new Datasource() { DatasourceName = name, SchemaName = schema, TableName = table, MapName = table, Query = query };
        d.KeyColumnsConfig = this.GetKeyColumns(schema, table);
        var c = (new DestinationRepository(Database, Connection) { Logger = Logger }).FindByName(destschema, desttable);
        if (c == null) throw new Exception();
        d.Destination = c;
        return d;
    }

    public void Save(Datasource d)
    {
        var dbcolumns = this.GetColumns("", TableName);
        d.DestinationId = d.Destination.DestinationId;

        var sq = SqlParser.Parse(d, nameconverter: x => x.ToSnakeCase().ToLower());
        //var config = JsonSerializer.Serialize(d.KeyColumns);
        //sq.Select.Add().Value(":key_cols_config").As("key_columns_config").AddParameter(":key_cols_config", config);

        sq.RemoveSelectItem(dbcolumns);

        SqlMapper.AddTypeHandler(new DictionaryTypeHandler());
        if (d.DatasourceId == 0)
        {
            var q = sq.ToInsertQuery(TableName, new() { IdColumn });
            q.CommandText += $"\r\nreturning {IdColumn}";
            Logger?.Invoke(q.ToDebugString());
            d.DatasourceId = Connection.Query<int>(q).First();
        }
        else
        {
            sq.Select.Add().Value("current_timestamp").As("updated_at");
            var q = sq.ToUpdateQuery(TableName, new() { IdColumn });
            Logger?.Invoke(q.ToDebugString());
            Connection.Execute(q);
        }
        SqlMapper.ResetTypeHandlers();

        d.Extensions.ForEach(x => Save(x));
    }

    public void CreateTableOrDefault()
    {
        var sql = @$"
create table if not exists kms_datasources (
    datasource_id serial8 not null primary key
    , datasource_name text not null
    , destination_id int8 not null 
    , description text not null
    , parent_datasource_id int8
    , group_name text not null
    , schema_name text not null
    , table_name text not null
    , map_name text
    , query text not null
    , key_columns_config text not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
    , unique(destination_id, datasource_name)
)";
        Connection.Execute(sql);
    }
}

