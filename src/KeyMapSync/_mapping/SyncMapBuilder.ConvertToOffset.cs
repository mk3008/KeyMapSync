using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync
{
    /// <summary>
    ///
    /// </summary>
    public partial class SyncMapBuilder
    {
        /// <summary>
        /// Temporarily export your changes and transfer them as a data source.
        /// The transfer result is recorded in the mapping table for offset.
        /// </summary>
        /// <param name="origindef"></param>
        /// <param name="opt"></param>
        /// <param name="version">validate this sync version or later</param>
        /// <returns></returns>
        public SyncMap ConvertToOffset(SyncMap origindef, IValidateOption opt, long version)
        {
            var def = GenerateValidateTemporarycMap(origindef, version);
            var offsetmap = GenerateOffsetDatasource(origindef, def.DatasourceTable.TableName, opt);

            //add
            def.DatasourceMap.Cascades.Add(offsetmap);

            return def;
        }

        private SyncMap GenerateValidateTemporarycMap(SyncMap origindef, long version)
        {
            var destId = origindef.DestinationTable.SequenceColumn.ColumnName;
            var verId = origindef.VersionTable.SequenceColumn.ColumnName;

            var mapping = origindef.MappingTable;
            var sql = $@"
{origindef.DatasourceMap.DatasourceQueryGenarator(null)},
_validate_target as (
    select
        d.{destId} as _{destId}
    from
        {origindef.DestinationTable.TableFullName} d
        inner join {origindef.SyncTable.TableFullName} s on d.{destId} = s.{destId}
        inner join {origindef.VersionTable.TableFullName} v on s.{verId} = v.{verId}
    where
        s.{verId} >= :_version
        and v.datasource_name = :_datasource_name
),
_validate_datasource as (
    select
        *
    from
        (
            select
                d.*
                , m.{destId} as _{destId}
            from
                {origindef.DatasourceMap.DatasourceAliasName} d
                inner join {origindef.MappingTable.TableFullName} m on {origindef.MappingTable.Columns.Where((x) => x != destId).Select((x) => $"d.{x} = m.{x}").ToString(" and ")}
        ) q
    where
        exists (select * from _validate_target x where x._{destId} = q._{destId})
)";
            Func<ExpandoObject> pgen = () =>
            {
                dynamic prm = (origindef.DatasourceMap.ParameterGenerator == null) ? new ExpandoObject() : origindef.DatasourceMap.ParameterGenerator();
                prm._version = version;
                prm._datasource_name = origindef.DatasourceName;
                return prm;
            };

            // auto create validation datasource
            var ds = new DatasourceMapWrap()
            {
                DestinationTableName = origindef.DestinationTable.TableName,
                MappingName = null,
                DatasourceQuery = sql,
                DatasourceAliasName = "_validate_datasource",
                DatasourceKeyColumns = null,
                ParameterGenerator = pgen,
                IsNeedExistsCheck = false,
                IsExtension = false,
            };

            var def = Build(ds);

            //no insert destination
            def.DestinationTable.TableName = null;

            return def;
        }

        private IDatasourceMap GenerateOffsetDatasource(SyncMap def, string tmpName, IValidateOption opt)
        {
            var idName = def.DestinationTable.SequenceColumn.ColumnName;
            var sql = $@"
with
datasource as (
    select
        --key columns
        {def.DestinationTable.Columns.Where((x) => !opt.ValueColumns.Contains(x)).Select((x) => $"d.{x}").ToString(",")}
        --value columns
        , {def.DestinationTable.Columns.Where((x) => opt.ValueColumns.Contains(x)).Select((x) => $"d.{x} * -1 as {x}").ToString(",")}
        --offsetted id
        , d.{idName} as offset_{idName}
    from
        {def.DestinationTable.TableFullName} d
        left join {tmpName} q on d.{idName} = q._{idName}
    where
        not (
            --validate
            {def.DestinationTable.Columns.Where((x) => x != idName && !opt.IgnoreColumns.Contains(x)).Select((x) => $"q.{x} = d.{x}").ToString(" and ")}
        )
        or
        q._{idName} is null
)";
            var ds = new DatasourceMapWrap()
            {
                DestinationTableName = def.DestinationTable.TableFullName,
                MappingName = "offset",
                DatasourceQuery = sql,
                DatasourceAliasName = "datasource",
                DatasourceKeyColumns = new string[] { $"offset_{idName}" },
                ParameterGenerator = null,
                IsNeedExistsCheck = false,
                IsExtension = false,
            };

            var offsetdef = Build(ds);

            //cascade datasource convert
            foreach (var item in def.DatasourceMap.Cascades.Where(x => x.IsExtension == false))
            {
                if (item.IsExtension == false)
                {
                    ds.Cascades.Add(item);
                }
                else
                {
                    var m = GenerateOffsetExtensionDatasource(def, offsetdef.MappingTable.TableFullName, item);
                    if (m != null) ds.Cascades.Add(m);
                }
            }

            return ds;
        }

        private IDatasourceMap GenerateOffsetExtensionDatasource(SyncMap def, string maptable, IDatasourceMap extensionmap)
        {
            var dest = DbExecutor.ReadTable(extensionmap.DestinationTableName);
            var idName = def.DestinationTable.SequenceColumn.ColumnName;

            // upper cascade is not supported.
            if (dest.Columns.Where(x => x == idName).Any() == false) return null;

            var sql = $@"
with
datasource as (
    select
        d.{idName}
        , {dest.Columns.Where(x => x != idName).Select(x => $"e.{x}").ToString(",")}
    from
        {def.DatasourceTable.TableName} d
        inner join {maptable} q on d.{idName} = q.{idName}
        inner join {extensionmap.DestinationTableName} e on q.offset_{idName} = e.{idName}
    where
        {dest.Columns.Where(x => x != idName).Select(x => $"e.{x} is not null").ToString(" and ")}
)";
            var ds = new DatasourceMapWrap()
            {
                DestinationTableName = extensionmap.DestinationTableName,
                MappingName = null,
                DatasourceQuery = sql,
                DatasourceAliasName = "datasource",
                DatasourceKeyColumns = null,
                ParameterGenerator = null,
                IsNeedExistsCheck = false,
                IsExtension = false,
            };

            return ds;
        }
    }
}