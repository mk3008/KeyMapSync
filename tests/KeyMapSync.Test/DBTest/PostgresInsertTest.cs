using Dapper;
using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using KeyMapSync.Test.Model.Postgres;
using Newtonsoft.Json.Bson;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KeyMapSync.Test.DBTest;

public class PostgresInsertTest
{

    private readonly ITestOutputHelper Output;

    public static string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

    public PostgresInsertTest(ITestOutputHelper output)
    {
        Output = output;

        using var cn = new NpgsqlConnection(CnString);
        cn.Open();
        using var tran = cn.BeginTransaction();
        TableManager.GetInitializeSqls().ForEach(x => cn.Execute(x));
        tran.Commit();
    }

    private Func<string, Destination> GetResolver()
    {
        var dic = new Dictionary<string, Destination>();

        using var cn = new NpgsqlConnection(CnString);
        cn.Open();

        var lst = new List<Destination>();
        lst.Add(DestinationManager.GetAccounts(cn));
        lst.Add(DestinationManager.GetExtDebitAccounts(cn));

        lst.ForEach(x => dic[x.DestinationName] = x);
        var resolver = (string x) => dic[x];

        return resolver;
    }

    private Dictionary<string, Datasource> GetDatasources()
    {
        var dic = new Dictionary<string, Datasource>();

        using var cn = new NpgsqlConnection(CnString);
        cn.Open();

        var lst = new List<Datasource>();
        lst.Add(DatasourceManager.GetSales(cn));
        lst.Add(DatasourceManager.GetPayments(cn));

        lst.ForEach(x => dic[x.TableName] = x);
        return dic;
    }

    private void CreateSystemTable(Synchronizer sync, List<Datasource> datasources)
    {
        using var cn = new NpgsqlConnection(CnString);
        cn.Open();
        datasources.ForEach(x => sync.CreateTable(cn, x));
    }

    private Results InsertSync(Synchronizer sync, Datasource datasource)
    {
        using var cn = new NpgsqlConnection(CnString);
        cn.Open();
        using var tran = cn.BeginTransaction();

        var result = sync.Insert(cn, datasource);

        tran.Commit();

        return result;
    }

    [Fact]
    public void InsertTest()
    {
        var resolver = GetResolver();
        IDBMS db = new Postgres();
        var sync = new Synchronizer(db, resolver);

        var datasources = GetDatasources();

        CreateSystemTable(sync, datasources.Values.ToList());

        // insert
        var res = InsertSync(sync, datasources["sales"]);

        Assert.Equal(5, res.Collection.Count());

        res = InsertSync(sync, datasources["payments"]);

        Assert.Equal(5, res.Collection.Count());
    }
}

