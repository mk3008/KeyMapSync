using System;
using System.Collections.Generic;

namespace KeyMapSync
{
    public class TemporaryTable
    {
        public string TableName { get; set; } = $"keymapsync_{DateTime.Now.ToString("yyyy_MM_dd_HH24_mm_ss_fff")}";

        public SequenceColumn DestinationSequence { get; set; }

        public string DatasourceQuery { get; set; }

        public string DatasourceAliasName { get; set; }

        public IEnumerable<string> SourceKeyColumns { get; set; }

        public Func<object> ParamGenerator { get; set; }
    }
}