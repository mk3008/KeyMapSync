using Dapper;
using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utf8Json;

namespace KeyMapSync.Transform
{
    public class DbManager
    {
        public DbManager(DbExecutor executor)
        {
            Executor = executor;
        }

        public DbExecutor Executor { get; }

        private IDictionary<string, Table> DestinationTables { get; set; } = new Dictionary<string, Table>();
        private IDictionary<string, Table> KeyMapTables { get; set; } = new Dictionary<string, Table>();
        private IDictionary<string, Table> OffsetMapTables { get; set; } = new Dictionary<string, Table>();
        private IDictionary<string, Table> SyncTables { get; set; } = new Dictionary<string, Table>();
        private IDictionary<string, Table> VersionTables { get; set; } = new Dictionary<string, Table>();

        public string GetKeyMapTableName(IDatasource datasource) => $"{datasource.DestinaionTableName}_map_{datasource.MappingName}";
        public string GetOffsetMapTableName(IDatasource datasource) => $"{datasource.DestinaionTableName}_offset";
        public string GetSyncTableName(IDatasource datasource) => $"{datasource.DestinaionTableName}_sync";
        public string GetNextTempName() => $"_temp_{DateTime.Now.ToString("ssffffff")}";
        public string GetNextBridgeName() => $"_bridge_{DateTime.Now.ToString("ssffffff")}";
        public string GetVersionTableName(IDatasource datasource) => $"{datasource.DestinaionTableName}_ver";
        public string GetVersionSeqColumnName(IDatasource datasource) => $"{GetVersionTableName(datasource)}_id";
        public string GetDatasourceColumnName() => "datasrouce_name";
        public string GetMappinfColumnName() => "mapping_name";

        public Table ReadDestinationTable(IDatasource datasource)
        {
            var name = datasource.DestinaionTableName;

            if (DestinationTables.ContainsKey(name)) return DestinationTables[name];

            var tbl = Executor.ReadTable(name);
            if (tbl == null) throw new InvalidOperationException($"Destination table is not exists.({name})");

            //cache
            DestinationTables.Add(name, tbl);
            return tbl;
        }

        public Table ReadOrCreateKeyMapTable(IDatasource datasource)
        {
            var name = GetKeyMapTableName(datasource);
            var dic = KeyMapTables;

            if (dic.ContainsKey(name)) return dic[name];

            var tbl = Executor.ReadTable(name);
            if (tbl == null)
            {
                //create
                var dest = ReadDestinationTable(datasource);

                Executor.CreateMappingTable(name, dest, datasource.DatasourceKeyColumns);
                tbl = Executor.ReadTable(name);
            }

            //cache
            dic.Add(name, tbl);
            return tbl;
        }

        public Table ReadOrCreateOffsetMapTable(IDatasource datasource)
        {
            var name = GetOffsetMapTableName(datasource);
            var dic = OffsetMapTables;

            if (dic.ContainsKey(name)) return dic[name];

            var tbl = Executor.ReadTable(name);
            if (tbl == null)
            {
                //create
                var dest = ReadDestinationTable(datasource);

                Executor.CreateVersionTable (name, GetVersionSeqColumnName(datasource), GetDatasourceColumnName(), GetMappinfColumnName());
                tbl = Executor.ReadTable(name);
            }

            //cache
            dic.Add(name, tbl);
            return tbl;
        }

        public Table ReadOrCreateSyncTable(IDatasource datasource)
        {
            var name = GetSyncTableName(datasource);
            var dic = SyncTables;

            if (dic.ContainsKey(name)) return dic[name];

            var tbl = Executor.ReadTable(name);
            if (tbl == null)
            {
                //create
                var dest = ReadDestinationTable(datasource);
                var ver = ReadOrCreateVersionTable(datasource);

                Executor.CreateSyncTable(name, dest, ver);
                tbl = Executor.ReadTable(name);
            }

            //cache
            dic.Add(name, tbl);
            return tbl;
        }

        public Table ReadOrCreateVersionTable(IDatasource datasource)
        {
            var name = GetVersionTableName(datasource);
            var dic = VersionTables;

            if (dic.ContainsKey(name)) return dic[name];

            var tbl = Executor.ReadTable(name);
            if (tbl == null)
            {
                //create
                Executor.CreateVersionTable(name, GetVersionSeqColumnName(datasource), GetDatasourceColumnName(), GetMappinfColumnName());
                tbl = Executor.ReadTable(name);
            }

            //cache
            dic.Add(name, tbl);
            return tbl;
        }

        public long GetNextVersion(Table ver)
        {
            var sql = $"select {ver.SequenceColumn.NextValCommand}";
            return Executor.Connection.ExecuteScalar<long>(sql);
        }

        public Table ReadTable(string table)
        {
            return Executor.ReadTable(table);
        }

        public IEnumerable<string> ReadColumns(string table)
        {
            return Executor.ReadColumns(table);
        }

        //public IEnumerable<string> ReadColumnsForce(DatasourceMap map)
        //{
        //    var tmp = GetNextTempName();

        //    var qs = map.ParameterSet ?? new ParameterSet();
        //    qs = qs.Merge(new ParameterSet() { ConditionSqlText = "1 <> 1" });
        //    var sql = $"create temporary table {tmp} as {map.WithQueryText} select * from {map.AliasName} {qs.ToWhereSqlText()}";

        //    //create table
        //    Executor.Connection.Execute(sql, qs.ToExpandObject());

        //    var cols = ReadColumns(tmp);

        //    //drop table
        //    Executor.Connection.Execute($"drop table {tmp}");

        //    return cols;
        //}

        //public IEnumerable<string> GetDatasourceColumns(DatasourceMap map)
        //{
        //    map.WithQueryText

        //    //create dummy table
        //    var tmp = DeepCopy(bridgemap);
        //    tmp.LoadParameterSet = tmp.LoadParameterSet.Merge(new ParameterSet() { ConditionSqlText = "1 <> 1" });

        //    //scan columns
        //    var q = tmp.ToLoadQuery();
        //    Executor.Connection.Execute(q);
        //    var cols = Executor.ReadColumns(name);

        //    //drop dummy table
        //    Executor.Connection.Execute($"drop table {name}");

        //    return cols;
        //}

        private T DeepCopy<T>(T instance)
        {
            using (var stream = new MemoryStream())
            {
                JsonSerializer.Serialize(stream, instance);
                return JsonSerializer.Deserialize<T>(stream);
            }
        }
    }
}
