using Dapper;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync
{
    internal static class IDbConnectionExtension
    {
        public static int Execute(this IDbConnection source, LoadQuery query, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return source.Execute(query.Sql, query.Parameter, transaction, commandTimeout, commandType);
        }
    }
}
