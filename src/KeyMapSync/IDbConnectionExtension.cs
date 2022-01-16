using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal static class IDbConnectionExtension
{
    public static int Execute(this IDbConnection source, (string commandText, IDictionary<string, object> parameter) command)
    {
        return source.Execute(command.commandText, command.parameter);
    }
}
