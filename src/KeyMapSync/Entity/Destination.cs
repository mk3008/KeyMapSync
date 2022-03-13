using KeyMapSync.DMBS;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity
{
    public class Destination
    {
        /// <summary>
        /// Destination table name.
        /// </summary>
        public string DestinationTableName { get; set; } = String.Empty;

        public List<GroupDestination> Groups { get; set; } = new();

        public Sequence Sequence { get; set; } = new();

        /// <summary>
        /// Destination columns.
        /// </summary>
        public List<string> Columns { get; set; } = new();

        public List<string> GetColumnsWithoutKey() => Columns.Where(x => x != Sequence.Column).ToList();


        public KeyMapConfig? KeyMapConfig { get; set; } = null;

        public VersioningConfig? VersioningConfig { get; set; } = null;

        public TablePair ToInsertTablePair(Datasource d, string? sequencePrefix = null)
        {
            var pair = new TablePair()
            {
                FromTable = d.BridgeName,
                ToTable = DestinationTableName
            };

            var seq = Sequence;
            pair.AddColumnPair($"{sequencePrefix}{seq.Column}", seq.Column);
            GetColumnsWithoutKey().Where(x => d.Columns.Contains(x)).ToList().ForEach(x => pair.AddColumnPair(x));

            pair.Where = (string.IsNullOrEmpty(sequencePrefix)) ? null : $"where {sequencePrefix}{seq.Column} is not null";

            return pair;
        }

        public SqlCommand ToInsertCommand(Datasource d, string? sequencePrefix = null)
        {
            var p = ToInsertTablePair(d, sequencePrefix);
            var cmd = p.ToInsertCommand().ToSqlCommand();

            return cmd;
        }
    }
}
