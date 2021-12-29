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
        private string DiffAliasName => "_diff";

        public BridgeLoad BuildDiffBridgeMapping(OffsetContainer wrap, BridgeLoad bridgemap)
        {
            var query = new BridgeLoad()
            {
                BridgeTableName = Manager.GetNextBridgeName(),
                AliasName = DiffAliasName,
                WithQueryText = GetDiffWithText(wrap, bridgemap),
                Filter = wrap.Datasource.ParameterSet ?? new ParameterSet(),
                Columns = Manager.GetBridgeColumns(bridgemap)
            };
            query.Filter = query.Filter.Merge(GetDiffFilter(wrap, query.Columns));

            //赤伝転送
            //remove
            //offsetkey

            return query;
        }

        private string GetDiffWithText(OffsetContainer wrap, BridgeLoad bridgemap)
        {
            var sql =
$@"with
{DiffAliasName} as (select * from {bridgemap.BridgeTableName})";

            return sql;
        }

        private ParameterSet GetDiffFilter(OffsetContainer wrap, IEnumerable<string> bridgecols)
        {
            var datasource = wrap.Datasource;
            var dest = Manager.ReadDestinationTable(datasource);

            var q = dest.Columns.Where(x => !wrap.IgnoreColumns.Contains(x) && !wrap.KeyColumns.Contains(x) && bridgecols.Contains(x));
            var s = q.Select(x => $"(({DiffAliasName}.{x} is null and _origin.{x} is null) or {DiffAliasName}.{x} = _origin.{x})").ToString(" and ");

            var sql = $"not exists (select * from {dest.TableFullName} _origin where {DiffAliasName}._{dest.SequenceColumn.ColumnName} and _origin.{dest.SequenceColumn.ColumnName} and {s})";
            return new ParameterSet() { ConditionSqlText = sql };
        }
    }
}
