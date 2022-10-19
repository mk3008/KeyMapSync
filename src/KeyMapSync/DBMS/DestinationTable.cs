using Dapper;
using SqModel;
using SqModel.Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class KeyValueTable
{
    public string TableName { get; set; }

    public string KeyName { get; set; }

    public string ValueName { get; set; }

    public string TimestampName { get; set; }

    public string? FindOrDefault(IDbConnection cn, string key)
    {
        var sq = new SelectQuery();
        var t = sq.From(TableName).As("t");
        sq.Where.Add().Column(t, KeyName).Equal(":name").AddParameter(":name", key);
        sq.Select.Add().Column(t, $"{ValueName}::text");
        return cn.Query<string>(sq.ToQuery()).FirstOrDefault();
    }

    public void Save(IDbConnection cn, string key, string value)
    {

    }
}
