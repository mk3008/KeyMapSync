using System;

namespace KeyMapSync
{
    /// <summary>
    /// sql info
    /// </summary>
    public class SqlResultArgs : EventArgs
    {
        public SqlResultArgs(string sql, int count, object param = null)
        {
            Sql = sql;
            Param = param;
            Count = count;
        }

        public string TableName { get; set; }

        public string Sql { get; set; }

        public object Param { get; set; }

        public int Count { get; set; }
    }
}