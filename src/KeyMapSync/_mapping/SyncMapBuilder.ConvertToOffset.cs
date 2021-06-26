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
        public SyncMap ConvertToOffset(SyncMap origindef, IValidateOption opt, long version)
        {
            var def = GenerateValidateSyncMap(origindef, version);
            var offsetmap = GenerateOffsetDatasource(origindef, def.DatasourceTable.TableName, opt);
            def.DatasourceMap.Cascades.Add(offsetmap);

            return def;
        }

        private SyncMap GenerateValidateSyncMap(SyncMap origindef, long version)
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
            Func<object> pgen = () =>
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
            var sql = @$"
with
datasource as (
    select
        {def.DestinationTable.Columns.Where((x) => !opt.PriceColumns.Contains(x)).Select((x) => $"d.{x}").ToString(",")}
        , {def.DestinationTable.Columns.Where((x) => opt.PriceColumns.Contains(x)).Select((x) => $"d.{x} * -1 as {x}").ToString(",")}
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

            return ds;
        }
    }
}