using Dapper;
using KeyMapSync;
using System.Data;
using System.Data.SQLite;

namespace SQLiteSample
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var cnstring = "Data Source=./database.sqlite;Cache=Shared";

            using (var cn = new SQLiteConnection(cnstring))
            {
                cn.Open();
                InitializeDatabase(cn);
            }

            using (var cn = new SQLiteConnection(cnstring))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                sync.Insert(new CustomerDatasourceMap());
                sync.Insert(new CorporationDatasourceMap());
            }
        }

        static private void InitializeDatabase(IDbConnection cn)
        {
            cn.Execute("create table if not exists customer (customer_id integer primary key autoincrement, customer_name text not null)");
            if (cn.ExecuteScalar<int>("select count(*) from customer") == 0)
            {
                cn.Execute("insert into customer (customer_name)values ('test1'), ('sampels2'), ('dummy3')");
            }

            cn.Execute("create table if not exists corporation (corporation_id integer primary key autoincrement, corporation_name text not null)");
            if (cn.ExecuteScalar<int>("select count(*) from corporation") == 0)
            {
                cn.Execute("insert into corporation (corporation_name)values ('test4.co.jp'), ('sampel5.com'), ('dummy6.net')");
            }

            cn.Execute("create table if not exists client(client_id integer primary key autoincrement, client_name text not null, remarks text)");
        }
    }
}