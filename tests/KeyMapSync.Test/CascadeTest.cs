using Dapper;
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

            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                //datasource
                cn.Execute("create table if not exists sales_data(sales_data_seq integer primary key autoincrement not null, sales_data_id integer not null, sales_data_row_id integer not null, sales_date date not null, product text not null, price integer not null, remarks text)");

                cn.Execute("delete from sales_data");
                cn.Execute(@"
insert into sales_data (
sales_data_id, sales_data_row_id, sales_date, product, price, remarks
)
values
  (1, 1, '2020/01/01', 'apple', 100, null)
, (1, 2, '2020/01/01', 'orange', 150, null)
, (1, 3, '2020/01/01', 'tomato', 1000, null)
, (2, 1, '2020/01/02', 'black tea', 100, 'assam')
, (2, 1, '2020/01/02', 'green tea', 100, null)
, (2, 2, '2020/01/02', 'coffee', 150, null)
;");

                //destination
                cn.Execute("create table if not exists sales (sales_id integer primary key autoincrement, sales_date date not null)");
                cn.Execute("create table if not exists sales_detail (sales_detail_id integer primary key autoincrement, sales_id integer not null, product text not null, price integer not null)");
                cn.Execute("create table if not exists sales_detail_ext_remarks (sales_detail_id integer not null, remarks not null)");

                cn.Execute("delete from sales");
                cn.Execute("delete from sales_detail");
                cn.Execute("delete from sales_detail_ext_remarks");
            }
        }

        public string CnString => "Data Source=./database_cascade.sqlite;Cache=Shared";

        [Fact]
        public void CascadeInsert()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                //log
                //DbExecutor.OnBeforeExecute += OnBeforeExecute;

                var ds = new Datasouce.SalesDetailDatasource();
                var def = builder.Build(ds);

                sync.Insert(def);
                var res = sync.Result;

                Assert.Equal(6, res.Count);
                Assert.Equal(1, res.InnerResults.Where(x => x.Definition.DestinationTable.TableName == "sales_detail_ext_remarks").First().Count);
                Assert.Equal(2, res.InnerResults.Where(x => x.Definition.DestinationTable.TableName == "sales").First().Count);
            }
        }

        //private void OnBeforeExecute(object sender, SqlEventArgs e)
        //{
        //    Output.WriteLine(e.Sql);
        //}
    }
}