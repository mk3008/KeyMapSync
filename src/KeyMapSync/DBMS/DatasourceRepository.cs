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

    public List<Datasource> Find(bool includeDisable, Action<SelectQuery, TableClause>? filter = null)
    {
        var sql = @"select
    d.datasource_id
    , d.datasource_name
    , d.destination_id
    , d.description
    , d.query
    , d.disable
    , r.group_name
    , e.parent_datasource_id
    , k.map_name
    , k.schema_name
    , k.table_name
    , k.key_columns_config
from
    kms_datasources d
    left join kms_datasource_roots r on d.datasource_id = r.datasource_id and d.is_root = true
    left join kms_datasource_extensions e on d.datasource_id = e.datasource_id and d.is_root = false
    left join kms_datasource_maps k on d.datasource_id = k.datasource_id and d.has_keymap = true";

        var sq = SqlParser.Parse(sql);
        sq.GetSelectItems().ForEach(x => x.Name = x.Name.Replace("_", ""));

        var d = sq.FromClause;

        if (includeDisable == false) sq.Where.Add().Column(d, "disable").False();
        filter?.Invoke(sq, d);
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

    public Datasource FindById(int id, bool includeDisable = false)
    {
        var lst = Find(includeDisable, (sq, t) =>
        {
            sq.Where.Add().Column(t, IdColumn).Equal(":id").AddParameter(":id", id);
        });

        return lst.First();
    }

    public Datasource? FindByName(string name, string destschame, string desttable, bool includeDisable = false)
    {
        var lst = Find(includeDisable, (sq, t) =>
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

    public List<Datasource> FindByParentId(int parentid, bool includeDisable = false)
    {
        var lst = Find(includeDisable, (sq, t) =>
        {
            sq.Where.Add().Column("e", "parent_datasource_id").Equal(":id").AddParameter(":id", parentid);
        });
        return lst;
    }

    public List<Datasource> FindByGroup(string group, bool includeDisable = false)
    {
        var lst = Find(includeDisable, (sq, t) =>
        {
            sq.Where.Add().Column("r", "group_name").Equal(":group").AddParameter(":group", group);
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

    public Datasource GetScaffold(string name, int parentDatasourceId, string query, string destschema, string desttable)
    {
        var d = new Datasource() { DatasourceName = name, ParentDatasourceId = parentDatasourceId, Query = query };
        var c = (new DestinationRepository(Database, Connection) { Logger = Logger }).FindByName(destschema, desttable);
        if (c == null) throw new Exception();
        d.Destination = c;
        return d;
    }

    public void Save(Datasource d)
    {
        var dbcolumns = this.GetColumns("", "kms_datasources");
        d.DestinationId = d.Destination.DestinationId;

        var sq = SqlParser.Parse(d, nameconverter: x => x.ToSnakeCase().ToLower());
        sq.RemoveSelectItem(dbcolumns);

        var dbdata = FindByName(d.DatasourceName, d.Destination.SchemaName, d.Destination.TableName, false);

        if (dbdata != null && d.DatasourceId != dbdata.DatasourceId)
        {
            throw new InvalidOperationException($"This name is already exists.(name : {d.DatasourceName}, destination : {d.Destination.TableFulleName})");
        }
        else if (dbdata == null)
        {
            var q = sq.ToInsertQuery(TableName, new() { IdColumn });
            q.CommandText += $"\r\nreturning {IdColumn}";
            Logger?.Invoke(q.ToDebugString());
            d.DatasourceId = Connection.Query<int>(q).First();
        }
        else
        {
            sq.Select.Add().Value("clock_timestamp()").As("updated_at");
            var q = sq.ToUpdateQuery(TableName, new() { IdColumn });
            Logger?.Invoke(q.ToDebugString());
            Connection.Execute(q);
        }

        if (d.ParentDatasourceId == null)
        {
            SaveExtension(d, "kms_datasource_roots");
            if (string.IsNullOrEmpty(d.MapName))
            {
                DeleteExtension(d, "kms_datasource_maps");
            }
            else
            {
                SaveExtension(d, "kms_datasource_maps");
            }
            DeleteExtension(d, "kms_datasource_extensions");
        }
        else
        {
            DeleteExtension(d, "kms_datasource_roots");
            DeleteExtension(d, "kms_datasource_maps");
            SaveExtension(d, "kms_datasource_extensions");
        }

        d.Extensions.ForEach(x => Save(x));
    }

    private void SaveExtension(Datasource d, string extable)
    {
        var dbcolumns = this.GetColumns("", extable);

        var sq = SqlParser.Parse(d, nameconverter: x => x.ToSnakeCase().ToLower());
        sq.RemoveSelectItem(dbcolumns);

        var cnt = Connection.ExecuteScalar<int>($"select count(*) from {extable} where datasource_id = :id", new { id = d.DatasourceId });

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

    private void DeleteExtension(Datasource d, string table)
    {
        Connection.Execute($"delete from {table} where datasource_id = :id", new { id = d.DatasourceId });
    }

    public void CreateTableOrDefault()
    {
        var sql = @$"
create table if not exists kms_datasources (
    datasource_id serial8 not null primary key
    , datasource_name text not null
    , destination_id int8 not null 
    , description text not null
    , query text not null
    , is_root bool not null
    , has_keymap bool not null
    , disable bool not null default false
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
    , unique(destination_id, datasource_name)
    , check(case when is_root = false and has_keymap = true then false else true end)
)
;
create table if not exists kms_datasource_roots (
    datasource_id int8 not null primary key
    , group_name text not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)
;
create table if not exists kms_datasource_maps (
    datasource_id int8 not null primary key
    , map_name text not null
    , schema_name text not null
    , table_name text not null
    , key_columns_config text not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)
;
create table if not exists kms_datasource_extensions (
    datasource_id int8 not null primary key
    , parent_datasource_id int8 not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)
";
        sql.Split(";").ToList().ForEach(x => Connection.Execute(x));
    }
}

