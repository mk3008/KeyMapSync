﻿using Dapper;
using System;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using Xunit;

namespace KeyMapSync.Test
{
    public class SyncMapBuilderTest
    {
        public string CnString => "Data Source=./database.sqlite;Cache=Shared";

        public SyncMapBuilderTest()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                cn.Execute("create table if not exists builder_test_source (builder_test_source_id integer primary key autoincrement, builder_test_source_name text not null)");
                cn.Execute("create table if not exists builder_test_destination(builder_test_destination_id integer primary key autoincrement, builder_test_destination_name text not null, remarks text)");
            }
        }

        [Fact]
        public void Default()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var def = builder.Build("builder_test_destination", "builder_test_source", "with datasource as (select builder_test_source_id, builder_test_source_name as builder_test_destination_name from builder_test_source)", new string[] { "builder_test_source_id" });

                //destination
                Assert.Equal("builder_test_destination", def.DestinationTable.TableFullName);
                Assert.Equal("builder_test_destination_id,builder_test_destination_name,remarks", string.Join(',', def.DestinationTable.Columns));
                Assert.Equal("builder_test_destination_id", def.DestinationTable.SequenceColumn.ColumnName);
                Assert.Equal("row_number() over() + (select max(seq) from (select seq from sqlite_sequence where name = 'builder_test_destination' union all select 0))", def.DestinationTable.SequenceColumn.NextValCommand);

                //sync
                Assert.Equal("builder_test_destination_sync", def.SyncTable.TableFullName);
                Assert.Equal("builder_test_destination_id,builder_test_destination_sync_version_id", string.Join(',', def.SyncTable.Columns));
                Assert.Null(def.SyncTable.SequenceColumn);

                //sync_version
                Assert.Equal("builder_test_destination_sync_version", def.VersionTable.TableFullName);
                Assert.Equal("builder_test_destination_sync_version_id,mapping_name,create_timestamp", string.Join(',', def.VersionTable.Columns));
                Assert.Equal("builder_test_destination_sync_version_id", def.VersionTable.SequenceColumn.ColumnName);
                Assert.Equal("row_number() over() + (select max(seq) from (select seq from sqlite_sequence where name = 'builder_test_destination_sync_version' union all select 0))", def.VersionTable.SequenceColumn.NextValCommand);

                //map
                Assert.Equal("builder_test_source", def.MappingName);
                Assert.Equal("builder_test_destination_map_builder_test_source", def.MappingTable.TableFullName);
                Assert.Equal("builder_test_source_id,builder_test_destination_id", string.Join(',', def.MappingTable.Columns));
                Assert.Null(def.MappingTable.SequenceColumn);

                //option
                Assert.True(def.HasKeycheck);

                //temporary
                Assert.StartsWith("builder_test_destination_map_builder_test_source", def.TemporaryTable.TableName);
                Assert.Equal("with datasource as (select builder_test_source_id, builder_test_source_name as builder_test_destination_name from builder_test_source)", def.TemporaryTable.DatasourceQuery);
                Assert.Equal("datasource", def.TemporaryTable.DatasourceAliasName);
                Assert.Equal(def.DestinationTable.SequenceColumn, def.TemporaryTable.DestinationSequence);
                Assert.Null(def.TemporaryTable.ParamGenerator);
                Assert.Equal("builder_test_source_id", string.Join(',', def.TemporaryTable.SourceKeyColumns));
            }
        }

        [Fact]
        public void AliasName()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var def = builder.Build("builder_test_destination", "builder_test_source", "with datasource1 as (select builder_test_source_id, builder_test_source_name as builder_test_destination_name from builder_test_source)", new string[] { "builder_test_source_id" }, "datasource1");

                //temporary
                Assert.Equal("with datasource1 as (select builder_test_source_id, builder_test_source_name as builder_test_destination_name from builder_test_source)", def.TemporaryTable.DatasourceQuery);
                Assert.Equal("datasource1", def.TemporaryTable.DatasourceAliasName);
            }
        }

        [Fact]
        public void Parameter()
        {
            using (var cn = new SQLiteConnection(CnString))
            {
                cn.Open();
                var exe = new DbExecutor(new SQLiteDB(), cn);
                var builder = new SyncMapBuilder() { DbExecutor = exe };
                var prm = new { value = 1 };
                var def = builder.Build("builder_test_destination", "builder_test_source", "with datasource as (select builder_test_source_id, builder_test_source_name as builder_test_destination_name from builder_test_source)", new string[] { "builder_test_source_id" }, paramGenerator: () => prm);

                Assert.Equal(prm, def.TemporaryTable.ParamGenerator.Invoke());
            }
        }
    }
}