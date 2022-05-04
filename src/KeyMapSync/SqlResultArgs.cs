using System;

namespace KeyMapSync
{
    /// <summary>
    /// sql info
    /// </summary>
    public class SqlResultArgs : EventArgs
    {
        public SqlResultArgs(SqlEventArgs owner, int count)
        {
            Owner = owner;
            Count = count;
            Timestamp = DateTime.Now;
        }

        public SqlEventArgs Owner { get; }

        public DateTime Timestamp { get; }

        public TimeSpan LapTime => Timestamp - Owner.Timestamp;

        public int Count { get;  }
    }
}