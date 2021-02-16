using Dapper;
using KeyMapSync;
using Npgsql;
using System.Data;

namespace PostgresSample
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var cnstring = "Server=localhost;Port=5432;Database=keymapsync;User ID=postgres;Password=postgres;Enlist=true";

            using (var cn = new NpgsqlConnection(cnstring))
            {
                cn.Open();
                InitializeDatabase(cn);
            }

            using (var cn = new NpgsqlConnection(cnstring))
            {
                cn.Open();

                var exe = new DbExecutor(new PostgresDB(), cn);
                var builder = new MappingDefinitionBuilder() { DbExecutor = exe, Destination = "client" };
                var dsList = new IDatasource[] { new CustomerDatasource(), new CorporationDatasource() };
                foreach (var ds in dsList)
                {
                    var def = builder.Build(ds);
                    var sync = new Synchronizer() { DbExecutor = exe };
                    sync.Insert(def);
                }
            }
        }

        static private void InitializeDatabase(IDbConnection cn)
        {
            cn.Execute("create table if not exists customer (customer_id serial8 primary key, customer_name text not null)");
            if (cn.ExecuteScalar<int>("select count(*) from customer") == 0)
            {
                cn.Execute("insert into customer (customer_name)values ('test1'), ('sampels2'), ('dummy3')");
            }

            cn.Execute("create table if not exists corporation (corporation_id serial8 primary key, corporation_name text not null)");
            if (cn.ExecuteScalar<int>("select count(*) from corporation") == 0)
            {
                cn.Execute("insert into corporation (corporation_name)values ('test4.co.jp'), ('sampel5.com'), ('dummy6.net')");
            }

            cn.Execute("create table if not exists client(client_id serial8, client_name text not null, remarks text)");
        }
    }
}