using Dapper;
using KeyMapSync;
using KeyMapSync.Entity;
using Npgsql;
using System;
using System.Data;

namespace PostgresSample;

internal class Program
{
    private static void Main(string[] args)
    {
        var cnstring = "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

        using (var cn = new NpgsqlConnection(cnstring))
        {
            cn.Open();
            //create table, and insert dummy data.
            DbInitializer.Execute(cn);
        };

        //generates management table DDL according to DBMS.
        var db = new KeyMapSync.DBMS.PostgreSQL();
        
        //transfer class.
        var sync = new Synchronizer(db);

        //hook debug event.
        sync.BeforeSqlExecute += OnBeforeSqlExecute;


        //execution of differential transfer.
        //if the transfer is successful, it will be committed, so reconnect the connection for each transfer.
        using (var cn = new NpgsqlConnection(cnstring))
        {
            cn.Open();
            sync.Insert(cn, CreateAsFishSales());
        };
        using (var cn = new NpgsqlConnection(cnstring))
        {
            cn.Open();
            sync.Insert(cn, CreateAsFruits());
        };
        using (var cn = new NpgsqlConnection(cnstring))
        {
            cn.Open();
            sync.Insert(cn, CreateAsFruitsCancel());
        };
    }

    private static Destination CreateAsIntegrationSales()
    {
        var d = new Destination();

        //destination table name.
        d.TableName = "integration_sales";

        //destination table columns.
        d.Columns = new() { "integration_sales_id", "sales_date", "product_name", "price" };

        //destination sequence column name and command.
        d.Sequence = new() { Column = "integration_sales_id", Command = "nextval('integration_sales_integration_sales_id_seq')" };

        //difference transfer support ON.
        d.KeyMapConfig = new();

        //offset support ON.
        //and set cancel column name.
        d.KeyMapConfig.OffsetConfig = new() { SignInversionColumns = new() { "price" } };

        //versioning support ON.
        d.VersioningConfig = new();
        d.VersioningConfig.Sequence = new() { Column = "version_id", Command = "nextval('integration_sales__version_version_id_seq')" };

        return d;
    }

    private static Datasource CreateAsFishSales()
    {
        var s = new Datasource();

        //datasource table name.
        s.TableName = "fish_sales";

        //datasource table key columns
        s.KeyColumns = new() { "fish_sales_id" };

        //datasource to destination column mapping query.include datasource key columns.
        s.Query = @"
select
    sales_date
    , fish_name as product_name
    , price
    --key info
    , fish_sales_id
from
    fish_sales";

        //datasource query columns.
        s.Columns = new() { "integration_sales_id", "sales_date", "product_name", "price", "fish_sales_id" };

        //destination.
        s.Destination = CreateAsIntegrationSales();

        //temporary table name.
        s.BridgeName = "_tmp";

        return s;
    }

    private static Datasource CreateAsFruits()
    {
        var s = new Datasource();

        //datasource table name.
        s.TableName = "fruits_sales";

        //datasource table key columns
        s.KeyColumns = new() { "fruits_sales_id" };

        //datasource to destination column mapping query.include datasource key columns.
        s.Query = @"
select
    sales_date
    , fruits_name as product_name
    , price
    --key info
    , fruits_sales_id
from
    fruits_sales";

        //datasource query columns.
        s.Columns = new() { "integration_sales_id", "sales_date", "product_name", "price", "fruits_sales_id" };

        //destination.
        s.Destination = CreateAsIntegrationSales();

        //temporary table name.
        s.BridgeName = "_tmp";

        return s;
    }

    private static Datasource CreateAsFruitsCancel()
    {
        var s = new Datasource();

        //datasource table name.
        //** it is not necessary to use the real table name because it is the name for the difference transfer judgment. **
        s.TableName = "fruits_sales_cancel";

        //datasource table key columns
        s.KeyColumns = new() { "fruits_sales_id" };

        //datasource to destination column mapping query.include datasource key columns.
        s.Query = @"
select
    sales_date
    , fruits_name as product_name
    , price
    --key info
    , fruits_sales_id
from
    fruits_sales
where
    delete_date is not null";

        //datasource query columns.
        s.Columns = new() { "integration_sales_id", "sales_date", "product_name", "price", "fruits_sales_id" };

        //destination.
        s.Destination = CreateAsIntegrationSales();

        //temporary table name.
        s.BridgeName = "_tmp";

        return s;
    }

    private static void OnBeforeSqlExecute(object? sender, SqlEventArgs? e)
    {
        if (e != null) Console.WriteLine(e.GetSqlInfo());
    }
}
