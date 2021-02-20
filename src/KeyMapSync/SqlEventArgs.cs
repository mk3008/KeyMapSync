using System;

namespace KeyMapSync
{
    /// <summary>
    /// sql info
    /// </summary>
    public class SqlEventArgs : EventArgs
    {
        public SqlEventArgs(string sql, object param = null)
        {
            Sql = sql;
            Param = param;
        }

        public string Sql { get; set; }

        public object Param { get; set; }
    }
}