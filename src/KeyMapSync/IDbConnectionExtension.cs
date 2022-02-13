using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal static class IDbConnectionExtension
{
    public static int Execute(this IDbConnection source, (string commandText, IDictionary<string, object>? parameter) command)
    {
        return source.Execute(command.commandText, command.parameter);
    }
    public static int Execute(this IDbConnection source, SqlCommand command)
    {
        return source.Execute(command.CommandText, command.Parameters);
    }

    public static T Transaction<T>(this IDbConnection source, Func<IDbTransaction, T> fn)
    {
        using var trn = source.BeginTransaction();
        var value = fn.Invoke(trn);
        trn.Commit();

        return value;
    }

    public static void Transaction(this IDbConnection source, Action<IDbTransaction> act)
    {
        using var trn = source.BeginTransaction();
        act.Invoke(trn);
        trn.Commit();
    }
}
