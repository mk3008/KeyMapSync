using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public static class IAbutmentSqlExtension
{
    public static string ToTemporaryViewDdl(this IAbutment source)
    {
        var sql = $@"create temporary view {source.ViewName}
as
{source.Datasource.Query};";
        return sql;
    }
}

