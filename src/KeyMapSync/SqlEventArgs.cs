using System;
using System.Collections.Generic;
using System.Linq;

namespace KeyMapSync
{
    /// <summary>
    /// sql info
    /// </summary>
    public class SqlEventArgs : EventArgs
    {
        public SqlEventArgs(SyncEventArgs owner, string name, string sql, object? param)
        {
            Owner = owner;
            Name = name;
            Sql = sql;
            Param = param;
        }

        public SyncEventArgs Owner { get; set; }

        public string Name { get; }

        public string Sql { get; }

        public object? Param { get; }

        public string GetSqlInfo()
        {
            var s = Sql;
            var dic = Param as Dictionary<string, object>;
            if (dic != null) s = $"{s};\r\n--{dic.Select(x => $"{x.Key} = {x.Value}").ToString(" and ")}";
            return s;
        }
    }
}