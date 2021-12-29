using Dapper;
using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform
{
    public partial class OffsetMapBuilder
    {

        private string ExpectAliasName => "_expect";

        private BridgeLoad BuildExpectBridgeMapping(OffsetContainer wrap)
        {
            var datasource = wrap.Datasource;
            wrap.Data
            var query = new BridgeLoad()
            {
                BridgeTableName = Manager.GetNextBridgeName(),
                AliasName = ExpectAliasName,
                WithQueryText = GetExpectWithText(wrap),
                Filter = wrap.Datasource.ParameterSet ?? new ParameterSet(),
                Columns = Manager.GetBridgeColumns(wrap.Datasource)
            };
            query.Filter = query.Filter.Merge(GetExpectVersionFilter(wrap));

            query.LoadQueryables.Add(BuildDiffBridgeMapping(wrap, query));

            return query;
        }

        private string GetExpectWithText(OffsetContainer wrap)
        {
            var datasource = wrap.Datasource;
            var dest = Manager.ReadDestinationTable(datasource);
            var sync = Manager.ReadOrCreateSyncTable(datasource);
            var ver = Manager.ReadOrCreateVersionTable(datasource);
            var keymap = Manager.ReadOrCreateKeyMapTable(datasource);

            //var values = dest.Columns.Where(x => x != dest.SequenceColumn.ColumnName && !wrap.IgnoreColumns.Contains(x) && !wrap.KeyColumns.Contains(x));

            var sql =
$@"{datasource.WithQueryText},
{ExpectAliasName} as (
    select 
        _d.*
        , _km.{dest.SequenceColumn.ColumnName} as _{dest.SequenceColumn.ColumnName}
        , _s.{ver.SequenceColumn.ColumnName} as _{ver.SequenceColumn.ColumnName}
    from 
        {datasource.AliasName} _d 
        inner join {keymap.TableName} _km on {datasource.DatasourceKeyColumns.Select(x => $"_d.{x} = _km.{x}").ToString(" and ")}
        inner join {sync.TableName} _s on _d.{dest.SequenceColumn.ColumnName} = _s.{dest.SequenceColumn.ColumnName}
)";

            return sql;
        }

        private ParameterSet GetExpectVersionFilter(OffsetContainer wrap)
        {
            var datasource = wrap.Datasource;
            var ver = Manager.ReadOrCreateVersionTable(datasource);

            var sql = $"_{ver.SequenceColumn.ColumnName} between :lower_version_id and :upper_version_id)";
            var dic = new ExpandoObject() as IDictionary<string, object>;
            dic.Add(":lower_version_id", wrap.LowerVersion);
            dic.Add(":upper_version_id", wrap.UpperVersion);

            return new ParameterSet() { ConditionSqlText = sql, Parameters = dic as ExpandoObject };
        }
    }
}
