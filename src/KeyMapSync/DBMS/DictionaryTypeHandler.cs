using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utf8Json;

namespace KeyMapSync.DBMS;

internal class DictionaryTypeHandler : SqlMapper.TypeHandler<Dictionary<string, DbColumn.Types>>
{
    public override void SetValue(IDbDataParameter parameter, Dictionary<string, DbColumn.Types> value)
    {
        parameter.DbType = DbType.String;
        parameter.Value = JsonSerializer.Serialize(value);
    }

    public override Dictionary<string, DbColumn.Types> Parse(object value)
    {
        var json = value?.ToString();
        if (json == null) return new();
        var c = JsonSerializer.Deserialize<Dictionary<string, DbColumn.Types>>(json);
        if (c == null) return new();
        return c;
    }
}
