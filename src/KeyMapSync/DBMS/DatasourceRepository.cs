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

public class DatasourceConfig
{
    public string SchemaName { get; set; } = "public";

    public string TableName { get; set; } = "kms_datasources";

    public string IdColumnName { get; set; } = "datasource_id";

    public string NameColumnName { get; set; } = "datasource_name";

    public string DestinationIdColumnName { get; set; } = "destination_id";

    public string CreateTimestampColumnName { get; set; } = "created_at";

    public string UpdateTimestampColumnName { get; set; } = "updated_at";

    public string TimestampCommand { get; set; } = "clock_timestamp()";

    public Dictionary<string, DbColumnType> Columns => new()
    {
        { "datasource_id", DbColumnType.Numeric },
        { "datasource_name", DbColumnType.Text },
        { "destination_id", DbColumnType.Numeric },
        { "description", DbColumnType.Text },
        { "query", DbColumnType.Text },
        { "extension_datasource_ids", DbColumnType.NumericArray },
        { "disable", DbColumnType.Bool },
        { "created_at", DbColumnType.Timestamp },
        { "updated_at", DbColumnType.Timestamp },
    };
}

public class RootDatasourceConfig
{
    public string SchemaName { get; set; } = "public";

    public string TableName { get; set; } = "kms_root_datasources";

    public string IdColumnName { get; set; } = "datasource_id";

    public string CreateTimestampColumnName { get; set; } = "created_at";

    public string UpdateTimestampColumnName { get; set; } = "updated_at";

    public string GroupColumnName { get; set; } = "group_name";

    public string TimestampCommand { get; set; } = "clock_timestamp()";

    public Dictionary<string, DbColumnType> Columns => new()
    {
        { "datasource_id", DbColumnType.Numeric },
        { "group_name", DbColumnType.Text },
        { "map_name", DbColumnType.Text },
        { "schema_name", DbColumnType.Text },
        { "table_name", DbColumnType.Text },
        { "key_columns_config", DbColumnType.Text },
        { "created_at", DbColumnType.Timestamp },
        { "updated_at", DbColumnType.Timestamp },
    };
}

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
    , d.extension_datasource_ids
    , r.group_name
    , r.map_name
    , r.schema_name
    , r.table_name
    , r.key_columns_config
from
    kms_datasources d
    left join kms_root_datasources r on d.datasource_id = r.datasource_id";

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
            x.ExtensionDatasourceIds.ToList().ForEach(y =>
            {
                x.Extensions.Add(FindById(y));
            });
        });
        return lst;
    }

    public Datasource FindById(long id, bool includeDisable = false)
    {
        var lst = Find(includeDisable, (sq, t) =>
        {
            sq.Where.Add().Column(t, IdColumn).Equal(":id").AddParameter(":id", id);
        });

        if (!lst.Any()) throw new Exception($"Datasource is not found.(id : {id}, includeDisable : {includeDisable})");

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

    public List<Datasource> FindByGroup(string group, Destination dest, bool includeDisable = false)
    {
        var lst = Find(includeDisable, (sq, t) =>
        {
            sq.Where.Add().Column("r", "group_name").Equal(":group").AddParameter(":group", group);
            sq.Where.Add().Column("d", "destination_id").Equal(":dest_id").AddParameter(":dest_id", dest.DestinationId);
        });
        return lst;
    }

    public Datasource SaveAsRoot(string group, string name, string schema, string table, string query, string destschema, string desttable)
    {
        var d = new Datasource() { GroupName = group, DatasourceName = name, SchemaName = schema, TableName = table, MapName = table, Query = query };
        d.KeyColumnsConfig = this.GetKeyColumns(schema, table);
        var c = (new DestinationRepository(Database, Connection) { Logger = Logger }).FindByName(destschema, desttable);
        if (c == null) throw new Exception($"Destination id not found. (schema : {destschema}, name : {desttable})");
        d.Destination = c;
        Save(d);
        return d;
    }

    public Datasource SaveAsExtension(string name, string query, string destschema, string desttable)
    {
        var d = new Datasource() { DatasourceName = name, Query = query };
        var c = (new DestinationRepository(Database, Connection) { Logger = Logger }).FindByName(destschema, desttable);
        if (c == null) throw new Exception($"Destination id not found. (schema : {destschema}, name : {desttable})");
        d.Destination = c;
        Save(d);
        return d;
    }

    public void Save(Datasource d)
    {
        var dbcolumns = this.GetColumns("", "kms_datasources");
        d.DestinationId = d.Destination.DestinationId;
        d.ExtensionDatasourceIds = d.Extensions.Select(x => x.DatasourceId).ToArray();

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

        if (d.IsRoot)
        {
            SaveExtension(d, "kms_root_datasources");
        }
        else
        {
            DeleteExtension(d, "kms_root_datasources");
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
    , extension_datasource_ids int8[] not null
    , disable bool not null default false
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
    , unique(destination_id, datasource_name)
)
;
create table if not exists kms_root_datasources (
    datasource_id int8 not null primary key
    , group_name text not null
    , map_name text not null
    , schema_name text not null
    , table_name text not null
    , key_columns_config text not null
    , created_at timestamp default current_timestamp
    , updated_at timestamp default current_timestamp
)
";
        sql.Split(";").ToList().ForEach(x => Connection.Execute(x));
    }
}

