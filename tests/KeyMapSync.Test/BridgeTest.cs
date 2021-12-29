using Dapper;
using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using KeyMapSync.Test.Model;
using KeyMapSync.Test.Script;
using KeyMapSync.Transform;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KeyMapSync.Test;

public class BridgeRootTest
{
    private readonly ITestOutputHelper Output;

    public static string CnString => "Data Source=./database.sqlite;Cache=Shared";

    public BridgeRootTest(ITestOutputHelper output)
    {
        Output = output;
        /*
                using (var cn = new SQLiteConnection(CnString))
                {
                    cn.Open();
                    foreach (var item in Integration.InitializeSql.Split(";"))
                    {
                        cn.Execute(item);
                    };
                    foreach (var item in EcShop.InitializeSql.Split(";"))
                    {
                        cn.Execute(item);
                    };
                    foreach (var item in EcShop.CreateDataSql.Split(";"))
                    {
                        cn.Execute(item);
                    };
                    foreach (var item in Store.InitializeSql.Split(";"))
                    {
                        cn.Execute(item);
                    };
                    foreach (var item in Store.CreateDataSql.Split(";"))
                    {
                        cn.Execute(item);
                    };
                }
        */
    }


    [Fact]
    public void All()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };

        Assert.Equal($@"{ds.WithQuery}
create table {tmp}
as
select * from ds;", root.ToSql());
    }

    [Fact]
    public void NotExistsAdditional()
    {
        var ds = EcShopSaleDetail.GetDatasource();
        var tmp = "tmp_parse";
        var root = new BridgeRoot() { Datasource = ds, BridgeName = tmp };
        var bridge = new Additional() { Owner = root, AdditionalCondition = new NotExistsKeyMapCondition() };

        Assert.Equal($@"{ds.WithQuery},
_added as (
    select (select max(seq) from (select seq from sqlite_sequence where name = 'integration_sales_detail' union all select 0)) as integration_sale_detail_id, ds.*
    from ds
    where not exists (select * from integration_sale_detail__map_ec_shop_sale_detail _km where ds.ec_shop_sale_detail_id = _km.ec_shop_sale_detail_id)
)
create table {tmp}
as
select * from _added;", bridge.ToSql());
    }

    [Fact]
    public void VersionOffsetParse()
    {
        var ds = EcShopSaleDetail.GetDatasource();

        var root = new BridgeRoot() { Datasource = ds, BridgeName = "tmp_default_parse" };
        var expect = new ExpectBridge() { Owner = root, Condition = new ExistsVersionRangeCondition() { MinVersion = 1, MaxVersion = 1 } };
        var bridge = new ChangedBridge() { Owner = expect };

        //_e.integration_sale_detail_id, _e.sale_date, _e.ec_shop_article_id, 
        //current_timestamp as create_timestamp,
        Assert.Equal($@"{ds.WithQuery},
_expect as (
    select _km.ec_shop_sale_detail_id, _origin.*
    from integration_sale_detail _origin
    inner join integration_sale_detail__map_ec_shop_sale_detail _km on _origin.integration_sale_detail_id = _km.integration_sale_detail_id
    where exists (select * from integration_sale_detail__sync _sync where _sync.version_id between :_min_version_id and :_max_version_id and _origin.integration_sale_detail_id = _sync.integration_sale_detail_id)
),
_changed as (
    select _e.article_name, _e.unit_price, _e.quantity * -1 as quantity, _e.price * -1 as price, case when ds.ec_shop_sale_detail_id is null then 'row:deleted' end || case when _e.sale_date <> ds.sale_date then 'sale_date:diff,' end || case when _e.unit_price <> ds.unit_price then 'unit_price:diff,' end || case when _e.price <> ds.price then 'price:diff,' end as _validate_remarks
    from _expect _e
    left join ds _e.ec_shop_sale_detail_id = ds.ec_shop_sale_detail_id
    where ds.ec_shop_sale_detail_id is null or _e.sale_date <> ds.sale_date or _e.unit_price <> ds.unit_price or _e.price <> ds.price
),
create table tmp01
as
select * from _validate", bridge.ToSql());
    }

    [Fact]
    public void HeaderDetailParse()
    {
        var head = new HeaderOption()
        {
            HeaderDestination = "integration_sale",
            Columns = new string[] { "sale_date" },
        };

        var ds = EcShopSaleDetail.GetDatasource();
        ds.HeaderOption = head;

        var bridge = new Additional();

        Assert.Equal($@"{ds.WithQuery},
_ds_1 as (
    select
        (select max(seq) from (select seq from sqlite_sequence where name = :_table_name union all select 0)) as integration_sale_detail_id, ds.*
    from
        ds
    where
        not exists (select * from integration_sale_detail__map_ec_shop_sale_detail km on ds.ec_shop_sale_detail_id = km.ec_shop_sale_detail_id)
),
_ds_2 as (
    select
        head.integration_sale_id, _ds_1.*
    from
        _ds_1
        (
            select
                ec_shop_sale_id, (select max(seq) from (select seq from sqlite_sequence where name = :_header_table_name union all select 0)) as integration_sale_id
            from
                _ds_1 
            group by 
                ec_shop_sale_id
        ) head on _ds_1.ec_shop_sale_id = head.ec_shop_sale_id
)
create table tmp01
as
select * from _ds_2", bridge.GetWithQuery());
    }

    [Fact]
    public void HeaderDetailVersionOffsetParse()
    {
        var head = new HeaderOption()
        {
            HeaderDestination = "integration_sale",
            Columns = new string[] { "sale_date" },
        };

        var ds = EcShopSaleDetail.GetDatasource();
        ds.HeaderOption = head;

        var bridge = new Additional();

        Assert.Equal($@"{ds.WithQuery},
_expect as (
    select
        _km.ec_shop_sale_detail_id, _origin.*, _head.sale_date
    from
        integration_sale_detail _origin
        inner join integration_sale _head on _origin.integration_sale_id = _head.integration_sale_id
        inner join integration_sale_detail__map_ec_shop_sale_detail _km on from.integration_sale_detail_id = _km.integration_sale_detail_id
    where
        exists (select * from integration_sale_detail__sync _sync where _sync.version_id between :_min_version_id and :_max_version_id and _origin.integration_sale_detail = _sync.integration_sale_detail)
),
_validate as (
    select
        _e.integration_sale_detail_id, _e.sale_date, _e.ec_shop_article_id, _e.article_name, _e.unit_price, _e.quantity * -1 as quantity, _e.price * -1 as price, current_timestamp as create_timestamp, case when ds.ec_shop_sale_detail_id is null then 'row:deleted' end || case when _e.sale_date <> ds.sale_date then 'sale_date:diff,' end || case when _e.unit_price <> ds.unit_price then 'unit_price:diff,' end || case when _e.price <> ds.price then 'price:diff,' end as _validate_remarks
    from
        _expect _e
        left join ds _e.ec_shop_sale_detail_id = ds.ec_shop_sale_detail_id
    where
        ds.ec_shop_sale_detail_id is null or _e.sale_date <> ds.sale_date or _e.unit_price <> ds.unit_price or _e.price <> ds.price
),
create table tmp01
as
select * from _validate", bridge.GetWithQuery());
    }

    //    [Fact]
    //    public void ValidateUpdate()
    //    {
    //        //DbExecutor.OnBeforeExecute += OnBeforeExecute;
    //        //DbExecutor.OnAfterExecute += OnAfterExecute;

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();
    //            var def = builder.Build(ds);

    //            sync.Insert(def);
    //            var res = sync.Result;
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            cn.Execute(@"
    //update sales_data set price = price * 2, remarks = 'navel' where sales_data_seq = 2
    //;
    //update sales_data set product = product || '2' where sales_data_seq = 3
    //;
    //delete from sales_data where sales_data_seq = 4
    //;
    //");
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();

    //            var validator = new Datasouce.SalesDetailValidate();
    //            var def = builder.BuildAsOffset(ds, validator);

    //            var summary = def.GetSummary();
    //            sync.Offset(ds, validator);
    //            var res = sync.Result;

    //            //Assert.Equal(3, res.InnerResults.First().Count);
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();
    //            var def = builder.Build(ds);

    //            sync.Insert(def);
    //            var res = sync.Result;

    //            Assert.Equal(2, res.Count);
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();

    //            var validator = new Datasouce.SalesDetailValidate();
    //            var def = builder.BuildAsOffset(ds, validator);

    //            var summary = def.GetSummary();
    //            sync.Offset(ds, validator);
    //            var res = sync.Result;

    //            //Assert.Equal(3, res.InnerResults.First().Count);
    //        }
    //    }

    //    [Fact]
    //    public void Filter()
    //    {
    //        //DbExecutor.OnBeforeExecute += OnBeforeExecute;
    //        //DbExecutor.OnAfterExecute += OnAfterExecute;

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();
    //            var def = builder.Build(ds);

    //            sync.Insert(def);
    //            var res = sync.Result;
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            cn.Execute(@"
    //update sales_data set price = price * 2 where product in ('apple', 'orange')
    //;
    //");
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();
    //            ds.ProductCondition = "apple";

    //            var validator = new Datasouce.SalesDetailValidate();
    //            var def = builder.BuildAsOffset(ds, validator);

    //            var summary = def.GetSummary();
    //            sync.Offset(ds, validator);
    //            var res = sync.Result;

    //            //Assert.Equal(3, res.InnerResults.First().Count);
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();
    //            var def = builder.Build(ds);

    //            sync.Insert(def);
    //            var res = sync.Result;
    //        }
    //    }

    //    [Fact]
    //    public void ValidateAllDelete()
    //    {
    //        //DbExecutor.OnBeforeExecute += OnBeforeExecute;
    //        //DbExecutor.OnAfterExecute += OnAfterExecute;

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();
    //            var def = builder.Build(ds);

    //            sync.Insert(def);
    //            var res = sync.Result;
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            cn.Execute(@"
    //delete from sales_data
    //;
    //");
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();

    //            var validator = new Datasouce.SalesDetailValidate();
    //            var def = builder.BuildAsOffset(ds, validator);

    //            var summary = def.GetSummary();
    //            sync.Offset(ds, validator);
    //            var res = sync.Result;

    //            //Assert.Equal(3, res.InnerResults.First().Count);
    //        }

    //        using (var cn = new NpgsqlConnection(CnString))
    //        {
    //            cn.Open();
    //            var exe = new DbExecutor(new PostgresDB(), cn);
    //            var builder = new SyncMapBuilder() { DbExecutor = exe };
    //            var sync = new Synchronizer(builder);

    //            var ds = new Datasouce.SalesDetailBridgeDatasource();
    //            var def = builder.Build(ds);

    //            sync.Insert(def);
    //            var res = sync.Result;

    //            Assert.Equal(0, res.Count);
    //        }
    //    }

    //    private void OnBeforeExecute(object sender, SqlEventArgs e)
    //    {
    //        if (e.Sql.IndexOf("insert into") == -1 && e.Sql.IndexOf("create temporary") == -1) return;

    //        Output.WriteLine(e.Sql);
    //        if (e.Param != null) Output.WriteLine(e.Param.ToString());
    //    }

    //    private void OnAfterExecute(object sender, SqlResultArgs e)
    //    {
    //        //if (e.Sql.IndexOf("insert into") == -1) return;

    //        Output.WriteLine($"{e.TableName} rows:{e.Count}");
    //    }
}
