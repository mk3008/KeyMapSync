using System;
using System.Collections.Generic;

namespace KeyMapSync
{
    public class Result
    {
        public TimeSpan Elapsed { get; set; }

        public SyncMap Definition { get; set; }

        public int Count { get; set; }

        public int? Version { get; set; }

        public IList<Result> InnerResults { get; } = new List<Result>();
    }
}