using Dapper;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace KeyMapSync.Test
{
    public class SynchronizerDeleteTest
    {
        public string CnString => "Data Source=./database_delete.sqlite;Cache=Shared";

        public SynchronizerDeleteTest()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                cn.Execute("create table if not exists customer (customer_id integer primary key autoincrement, customer_name text not null)");
                cn.Execute("create table if not exists corporation (corporation_id integer primary key autoincrement, corporation_name text not null)");
                cn.Execute("create table if not exists client(client_id integer primary key autoincrement, client_name text not null, remarks text)");
            }
        }

        [Fact]
        public void DeleteDestinationID()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);
                var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });

                //datasource 2rows inserted
                cn.Execute("insert into customer(customer_id, customer_name) values(1, 'test1'), (2, 'test2')");
                sync.Insert(def);

                var destinationId = cn.ExecuteScalar<int>("select client_id from client_map_customer where customer_id = 1");

                //row alive
                var cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id = :destinationId", new { destinationId });
                Assert.Equal(1, cnt);

                //delete
                sync.DeleteByDestinationId(def, destinationId);
                Assert.Equal(1, sync.Result.Count);

                //row deleted
                cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id = :destinationId", new { destinationId });
                Assert.Equal(0, cnt);

                cnt = cn.ExecuteScalar<int>("select count(*) from client_map_customer where client_id = :destinationId", new { destinationId });
                Assert.Equal(0, cnt);

                cnt = cn.ExecuteScalar<int>("select count(*) from client_sync where client_id = :destinationId", new { destinationId });
                Assert.Equal(0, cnt);
            }

            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);
                var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });

                //re insert (new destionation_id)
                sync.Insert(def);
                Assert.Equal(1, sync.Result.Count);
                var destinationId = cn.ExecuteScalar<int>("select client_id from client_map_customer where customer_id = 1");

                //row alive
                var cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id = :destinationId", new { destinationId });
                Assert.Equal(1, cnt);
            }
        }

        [Fact]
        public void DeleteVersionID()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);
                var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });

                //datasource 2rows inserted
                cn.Execute("insert into customer(customer_id, customer_name) values(3, 'test3'), (4, 'test4')");
                sync.Insert(def);

                var versionId = sync.Result.Version.Value;

                //row alive
                var syncCount = cn.ExecuteScalar<int>("select count(*) from client_sync where client_sync_version_id = :versionId", new { versionId });
                Assert.Equal(sync.Result.Count, syncCount);

                var cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id in (select client_id from client_map_customer where customer_id =3)");
                Assert.Equal(1, cnt);
                cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id in (select client_id from client_map_customer where customer_id =4)");
                Assert.Equal(1, cnt);

                //delete
                sync.DeleteByVersionId(def, versionId);
                Assert.Equal(sync.Result.Count, syncCount);

                //row deleted
                cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id in (select client_id from client_sync where client_sync_version_id = :versionId)", new { versionId });
                Assert.Equal(0, cnt);

                cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id in (select client_id from client_map_customer where customer_id =3)");
                Assert.Equal(0, cnt);
                cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id in (select client_id from client_map_customer where customer_id =4)");
                Assert.Equal(0, cnt);

                cnt = cn.ExecuteScalar<int>("select count(*) from client_map_customer where client_id in (select client_id from client_sync where client_sync_version_id = :versionId)", new { versionId });
                Assert.Equal(0, cnt);

                cnt = cn.ExecuteScalar<int>("select count(*) from client_sync where client_id in (select client_id from client_sync where client_sync_version_id = :versionId)", new { versionId });
                Assert.Equal(0, cnt);
            }

            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);
                var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });

                //re insert
                sync.Insert(def);
                Assert.Equal(2, sync.Result.Count);

                //row alive
                var cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id in (select client_id from client_map_customer where customer_id =3)");
                Assert.Equal(1, cnt);
                cnt = cn.ExecuteScalar<int>("select count(*) from client where client_id in (select client_id from client_map_customer where customer_id =4)");
                Assert.Equal(1, cnt);
            }
        }
    }
}