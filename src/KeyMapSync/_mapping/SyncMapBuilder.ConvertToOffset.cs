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
        public SyncMap BuildAsOffset(IDatasourceMappable ds, IValidateOption opt, int version = 0)
        {
            var origindef = Build(ds);
            var def = ConvertToOffset(origindef, opt, version);
            return def;
        }

        /// <summary>
        /// Temporarily export your changes and transfer them as a data source.
        /// The transfer result is recorded in the mapping table for offset.
        /// </summary>
        /// <param name="origindef"></param>
        /// <param name="opt"></param>
        /// <param name="version">validate this sync version or later</param>
        /// <returns></returns>
        public SyncMap ConvertToOffset(SyncMap origindef, IValidateOption opt, int version)
        {
            var def = GenerateExpectBridge(origindef, version);
            var offsetdef = GenerateOffset(origindef, def.BridgeTableName, opt, version);

            //add
            //def.DatasourceMap.Cascades.Add(offsetmap);

            def.Cascades.Add(offsetdef);

            return def;
        }

        private SyncMap GenerateExpectBridge(SyncMap origindef, int version)
        {
            var destId = origindef.DestinationTable.SequenceColumn.ColumnName;
            var verId = origindef.KeyMap.VersionTable.SequenceColumn.ColumnName;

            var mapping = origindef.KeyMap.MappingTable;

            var sql = $@"
{origindef.DatasourceMap.DatasourceQueryGenarator(null)},
_validate_target as (
    select
        d.{destId} as _{destId}
    from
        {origindef.DestinationTable.TableFullName} d
        inner join {origindef.KeyMap.SyncTable.TableFullName} s on d.{destId} = s.{destId}
        inner join {origindef.KeyMap.VersionTable.TableFullName} v on s.{verId} = v.{verId}
    where
        s.{verId} >= :_version
        and v.datasource_name = :_datasource_name
        and exists (select * from {mapping.TableFullName} x where d.{destId} = x.{destId})
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
                inner join {origindef.KeyMap.MappingTable.TableFullName} m on {origindef.KeyMap.MappingTable.Columns.Where((x) => x != destId).Select((x) => $"d.{x} = m.{x}").ToString(" and ")}
        ) q
    where
        exists (select * from _validate_target x where x._{destId} = q._{destId})
)";
            Func<ExpandoObject> pgen = () =>
            {
                dynamic prm = (origindef.DatasourceMap.ParameterGenerator == null) ? new ExpandoObject() : origindef.DatasourceMap.ParameterGenerator();
                prm._version = version;
                prm._datasource_name = origindef.DatasourceMap.ActualDatasourceType.FullName;
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
                IsBridge = true,
                Filter = origindef.DatasourceMap.Filter,
            };

            var def = Build(ds, prefix: "expect_");
            def.MustCascade = true;

            return def;
        }

        private SyncMap GenerateOffset(SyncMap origindef, string tmpName, IValidateOption opt, int version)
        {
            var destId = origindef.DestinationTable.SequenceColumn.ColumnName;
            var verId = origindef.KeyMap.VersionTable.SequenceColumn.ColumnName;

            var mapping = origindef.KeyMap.MappingTable;

            var idName = origindef.DestinationTable.SequenceColumn.ColumnName;
            var sql = $@"
with
_validate_target as (
    select
        d.{destId} as _{destId}
    from
        {origindef.DestinationTable.TableFullName} d
        inner join {origindef.KeyMap.SyncTable.TableFullName} s on d.{destId} = s.{destId}
        inner join {origindef.KeyMap.VersionTable.TableFullName} v on s.{verId} = v.{verId}
    where
        s.{verId} >= :_version
        and v.datasource_name = :_datasource_name
        and exists (select * from {mapping.TableFullName} x where d.{destId} = x.{destId})
),
datasource as (
    select
        --key columns
        {origindef.DestinationTable.Columns.Where((x) => idName != x).Where((x) => !opt.ValueColumns.Contains(x)).Select((x) => $"d.{x}").ToString(",")}
        --value columns
        , {origindef.DestinationTable.Columns.Where((x) => opt.ValueColumns.Contains(x)).Select((x) => $"d.{x} * -1 as {x}").ToString(",")}
        --offsetted id
        , d.{idName} as offset_{idName}
    from
        (
            select * from {origindef.DestinationTable.TableFullName} d where exists (select * from _validate_target x where x._{destId} = d.{destId})
        ) d
        left join {tmpName} q on d.{idName} = q._{idName}
    where
        not (
            --validate
            {origindef.DestinationTable.Columns.Where((x) => x != idName && !opt.IgnoreColumns.Contains(x)).Select((x) => $"q.{x} = d.{x}").ToString(" and ")}
        )
        or
        q._{idName} is null
)";
            Func<ExpandoObject> pgen = () =>
            {
                dynamic prm = (origindef.DatasourceMap.ParameterGenerator == null) ? new ExpandoObject() : origindef.DatasourceMap.ParameterGenerator();
                prm._version = version;
                prm._datasource_name = origindef.DatasourceMap.ActualDatasourceType.FullName;
                return prm;
            };

            var ds = new DatasourceMapWrap()
            {
                DestinationTableName = origindef.DestinationTable.TableName,
                MappingName = "offset",
                DatasourceQuery = sql,
                DatasourceAliasName = "datasource",
                DatasourceKeyColumns = new string[] { $"offset_{idName}" },
                ParameterGenerator = pgen,
                IsNeedExistsCheck = false,
                IsExtension = false,
                Filter = origindef.DatasourceMap.Filter,
            };

            var mapname = Build(ds).KeyMap.MappingTable.TableFullName;

            //cascade datasource convert
            foreach (var item in origindef.DatasourceMap.Cascades.Where(x => x.IsUpperCascade == false))
            {
                if (item.GetType() == origindef.DatasourceMap.ActualDatasourceType) continue;

                if (item.IsExtension == false)
                {
                    ds.Cascades.Add(item);
                }
                else
                {
                    var m = GenerateOffsetExtensionDatasource(origindef, mapname, item);
                    if (m != null) ds.Cascades.Add(m);
                }
            }

            var def = Build(ds, prefix: "offset_");
            def.Origin = origindef;

            return def;
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
        {def.BridgeTableName} d
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