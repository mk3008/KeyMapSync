using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KeyMapSync.Test
{
    public class CascadeTest
    {
        private readonly ITestOutputHelper Output;

        public CascadeTest(ITestOutputHelper output)
        {
            Output = output;

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();

                //datasource
                cn.Execute("drop table if exists sales_data");
                cn.Execute("create table if not exists sales_data(sales_data_seq serial8 not null primary key, sales_data_id integer not null, sales_data_row_id integer not null, sales_date date not null, product text not null, amount integer not null, price integer not null, remarks text)");

                //cn.Execute("delete from sales_data");
                cn.Execute(@"
insert into sales_data (
sales_data_id, sales_data_row_id, sales_date, product, amount, price, remarks
)
values
  (1, 1, '2020/01/01', 'apple'    ,  1,  100, null)
, (1, 2, '2020/01/01', 'orange'   ,  3,  150, 'valencia')
, (1, 3, '2020/01/01', 'tomato'   , 10, 1000, null)
, (2, 1, '2020/01/02', 'black tea',  2,  100, 'assam')
, (2, 2, '2020/01/02', 'green tea',  4,  100, null)
, (2, 3, '2020/01/02', 'coffee'   ,  5,  150, null)
;");

                //destination
                cn.Execute("drop table if exists sales");
                cn.Execute("drop table if exists sales_detail");
                cn.Execute("drop table if exists sales_detail_ext_remarks");
                cn.Execute("drop table if exists stock_detail");

                cn.Execute("create table if not exists sales (sales_id serial8 not null primary key, sales_date date not null)");
                cn.Execute("create table if not exists sales_detail (sales_detail_id serial8 not null primary key, sales_id integer not null, product text not null, amount integer not null, price integer not null)");
                cn.Execute("create table if not exists sales_detail_ext_remarks (sales_detail_id integer not null, remarks text not null)");
                cn.Execute("create table if not exists stock_detail (stock_detail_id serial8 not null primary key, sales_date date not null, product text not null, amount integer not null)");

                //cn.Execute("delete from sales");
                //cn.Execute("delete from sales_detail");
                //cn.Execute("delete from sales_detail_ext_remarks");

                cn.Execute("drop table if exists sales_detail_map_sales_data");
                cn.Execute("drop table if exists sales_detail_map_offset");
                cn.Execute("drop table if exists sales_map_sales_data");
                cn.Execute("drop table if exists stock_detail_map_sales_detail");

                cn.Execute("drop table if exists sales_detail_sync");
                cn.Execute("drop table if exists sales_detail_sync_version");
                cn.Execute("drop table if exists sales_sync");
                cn.Execute("drop table if exists sales_sync_version");
                cn.Execute("drop table if exists stock_detail_sync");
                cn.Execute("drop table if exists stock_detail_sync_version");
            }
        }

        public string CnString => "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

        [Fact]
        public void CascadeInsert()
        {
            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                //log
                //DbExecutor.OnBeforeExecute += OnBeforeExecute;

                var ds = new Datasouce.SalesDetailBridgeDatasource();
                var def = builder.Build(ds);

                //var summary = builder.GetSummary(def);

                sync.Insert(def);
                var res = sync.Result;

                Assert.Equal(6, res.Count);
                Assert.Equal(2, res.All().Where(x => x.Definition.DestinationTable.TableName == "sales_detail_ext_remarks").First().Count);
                Assert.Equal(2, res.InnerResults.Where(x => x.Definition.DestinationTable.TableName == "sales").First().Count);
            }
        }

        [Fact]
        public void ValidateUpdate()
        {
            //DbExecutor.OnBeforeExecute += OnBeforeExecute;
            //DbExecutor.OnAfterExecute += OnAfterExecute;

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var ds = new Datasouce.SalesDetailBridgeDatasource();
                var def = builder.Build(ds);

                sync.Insert(def);
                var res = sync.Result;
            }

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                cn.Execute(@"
update sales_data set price = price * 2, remarks = 'navel' where sales_data_seq = 2
;
update sales_data set product = product || '2' where sales_data_seq = 3
;
delete from sales_data where sales_data_seq = 4
;
");
            }

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var ds = new Datasouce.SalesDetailBridgeDatasource();

                var validator = new Datasouce.SalesDetailValidate();
                var def = builder.BuildAsOffset(ds, validator);

                var summary = def.GetSummary();
                sync.Offset(ds, validator);
                var res = sync.Result;

                //Assert.Equal(3, res.InnerResults.First().Count);
            }

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var ds = new Datasouce.SalesDetailBridgeDatasource();
                var def = builder.Build(ds);

                sync.Insert(def);
                var res = sync.Result;

                Assert.Equal(2, res.Count);
            }

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var ds = new Datasouce.SalesDetailBridgeDatasource();

                var validator = new Datasouce.SalesDetailValidate();
                var def = builder.BuildAsOffset(ds, validator);

                var summary = def.GetSummary();
                sync.Offset(ds, validator);
                var res = sync.Result;

                //Assert.Equal(3, res.InnerResults.First().Count);
            }
        }

        [Fact]
        public void ValidateAllDelete()
        {
            //DbExecutor.OnBeforeExecute += OnBeforeExecute;
            //DbExecutor.OnAfterExecute += OnAfterExecute;

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var ds = new Datasouce.SalesDetailBridgeDatasource();
                var def = builder.Build(ds);

                sync.Insert(def);
                var res = sync.Result;
            }

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                cn.Execute(@"
delete from sales_data
;
");
            }

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var ds = new Datasouce.SalesDetailBridgeDatasource();

                var validator = new Datasouce.SalesDetailValidate();
                var def = builder.BuildAsOffset(ds, validator);

                var summary = def.GetSummary();
                sync.Offset(ds, validator);
                var res = sync.Result;

                //Assert.Equal(3, res.InnerResults.First().Count);
            }

            using (var cn = new NpgsqlConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var ds = new Datasouce.SalesDetailBridgeDatasource();
                var def = builder.Build(ds);

                sync.Insert(def);
                var res = sync.Result;

                Assert.Equal(0, res.Count);
            }
        }

        private void OnBeforeExecute(object sender, SqlEventArgs e)
        {
            if (e.Sql.IndexOf("insert into") == -1 && e.Sql.IndexOf("create temporary") == -1) return;

            Output.WriteLine(e.Sql);
            if (e.Param != null) Output.WriteLine(e.Param.ToString());
        }

        private void OnAfterExecute(object sender, SqlResultArgs e)
        {
            //if (e.Sql.IndexOf("insert into") == -1) return;

            Output.WriteLine($"{e.TableName} rows:{e.Count}");
        }
    }
}