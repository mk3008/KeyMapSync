using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utf8Json;

namespace KeyMapSync.DBMS;

public class SequenceTypeHandler : SqlMapper.TypeHandler<Sequence>
{
    public override void SetValue(IDbDataParameter parameter, Sequence value)
    {
        parameter.DbType = DbType.String;
        parameter.Value = JsonSerializer.Serialize(value);
    }

    public override Sequence Parse(object value)
    {
        var json = value?.ToString();
        if (json == null) return new();
        var c = JsonSerializer.Deserialize<Sequence>(json);
        if (c == null) return new();
        return c;
    }
}