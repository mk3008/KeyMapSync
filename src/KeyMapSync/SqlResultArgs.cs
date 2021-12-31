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
        }

        public SqlEventArgs Owner { get; }

        public int Count { get;  }
    }
}