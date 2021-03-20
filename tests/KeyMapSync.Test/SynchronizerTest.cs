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
    public class SynchronizerTest
    {
        public string CnString => "Data Source=./database.sqlite;Cache=Shared";

        public SynchronizerTest()
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
        public void NoData()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var def = builder.Build("client", "customer", "with datasource as (select * from customer)", new string[] { "customer_id" });

                sync.Insert(def);
                var res = sync.Result;

                //result
                Assert.Null(res);

                //no data
                var cnt = cn.Execute("select count(*) from client");
                Assert.Equal(-1, cnt);
            }

            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });

                //datasource 1row inserted
                cn.Execute("insert into customer(customer_name) values('test')");
                sync.Insert(def);

                var res = sync.Result;
                Assert.Equal(1, res.Count);
                Assert.Equal(1, res.Version);
            }
        }

        [Fact]
        public void SingleRowSync()
        {
            var name = "SingleRowSync";

            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var def = builder.Build("client", "customer", $"with datasource as (select customer_id, customer_name as client_name from customer where customer_name = '{name}')", new string[] { "customer_id" });

                //datasource 1row inserted
                cn.Execute($"insert into customer(customer_name) values('{name}')");
                sync.Insert(def);

                var res = sync.Result;
                Assert.Equal(1, res.Count);

                var sql = $@"
select
    c.*
from
    client c
    inner join client_sync s on c.client_id = s.client_id
    inner join client_sync_version v on s.client_sync_version_id = v.client_sync_version_id
    inner join client_map_customer map on c.client_id = map.customer_id
where
    v.client_sync_version_id = :version
";
                var cnt = 0;
                var q = cn.Query(sql, new { version = res.Version });
                foreach (var row in q)
                {
                    Assert.Equal(name, row.client_name);
                    cnt++;
                }
                Assert.Equal(1, cnt);
            }
        }

        [Fact]
        public void ManyRowsSync()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer where customer_name like 'ManyRowsSync%')", new string[] { "customer_id" });

                //datasource 2rows inserted
                cn.Execute("insert into customer(customer_name) values('ManyRowsSync1'), ('ManyRowsSync2')");
                sync.Insert(def);

                var res = sync.Result;
                Assert.Equal(2, res.Count);

                var sql = $@"
select
    c.*
from
    client c
    inner join client_sync s on c.client_id = s.client_id
    inner join client_sync_version v on s.client_sync_version_id = v.client_sync_version_id
    inner join client_map_customer map on c.client_id = map.customer_id
where
    v.client_sync_version_id = :version
order by
    c.client_name
";
                var cnt = 0;
                var q = cn.Query(sql, new { version = res.Version });
                foreach (var row in q)
                {
                    if (cnt == 0) Assert.Equal("ManyRowsSync1", row.client_name);
                    if (cnt == 1) Assert.Equal("ManyRowsSync2", row.client_name);
                    cnt++;
                }
                Assert.Equal(2, cnt);
            }
        }

        [Fact]
        public void TemporaryExistsException()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });
                sync.Insert(def);

                //An error will occur if you try to synchronize the same mapping definition more than once
                //with the same connection.
                //The reason is that the temporary table already exists.
                //Please disconnect the connection once, discard the temporary table, and then synchronize.
                Assert.ThrowsAny<Exception>(() =>
                {
                    sync.Insert(def);
                });
            }
        }

        [Fact]
        public void ManualTransaction()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();

                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var sync = new Synchronizer(builder);

                var sql = $@"
select
    count(*)
from
    client c
    inner join client_sync s on c.client_id = s.client_id
    inner join client_sync_version v on s.client_sync_version_id = v.client_sync_version_id
    inner join client_map_customer map on c.client_id = map.customer_id
where
    v.client_sync_version_id = :version
order by
    c.client_name
";

                //1row
                cn.Execute("insert into customer(customer_name) values('tran1')");

                using (var tran = cn.BeginTransaction())
                {
                    var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });
                    sync.Insert(def, tran);
                    tran.Commit();
                }

                var res = sync.Result;
                var cnt = cn.ExecuteScalar<int>(sql, new { version = res.Version });
                Assert.Equal(1, cnt);

                //1row
                cn.Execute("insert into customer(customer_name) values('tran2')");

                using (var tran = cn.BeginTransaction())
                {
                    var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });
                    sync.Insert(def, tran);
                    // not comiit
                }

                res = sync.Result;
                cnt = cn.ExecuteScalar<int>(sql, new { version = res.Version });
                Assert.Equal(0, cnt);

                using (var tran = cn.BeginTransaction())
                {
                    var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer)", new string[] { "customer_id" });
                    sync.Insert(def, tran);
                    tran.Commit();
                }

                res = sync.Result;
                cnt = cn.ExecuteScalar<int>(sql, new { version = res.Version });
                Assert.Equal(1, cnt);
            }
        }

        [Fact]
        public void KeyExistsException()
        {
            //Suppose there is one or more data to be transferred
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                cn.Execute("insert into customer(customer_name) values('KeyExistsException')");
            }

            void fn()
            {
                using (var cn = new SQLiteConnection(CnString))
                {
                    cn.Open();

                    var exe = new DbExecutor(new SQLiteDB(), cn);
                    var builder = new SyncMapBuilder() { DbExecutor = exe };
                    var sync = new Synchronizer(builder);

                    var def = builder.Build("client", "customer", "with datasource as (select customer_id, customer_name as client_name from customer where customer_name = 'KeyExistsException')", new string[] { "customer_id" }, isNeedExistsCheck: false);
                    sync.Insert(def);
                }
            }

            fn();

            Assert.ThrowsAny<Exception>(() =>
            {
                //If you try to insert twice with "isNeedExistsCheck = false" without implementing existence check,
                //you will always get an error.
                fn();
            });
        }
    }
}