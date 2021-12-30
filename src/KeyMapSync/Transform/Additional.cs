﻿using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public class Additional : IBridge
{
    public IBridge Owner { get; set; }

    public Datasource Datasource => Owner.Datasource;

    public IAdditionalCondition AdditionalCondition { get; set; }

    public string Alias => "_added";

    public string BridgeName => Owner.BridgeName;

    public string GetWithQuery() => Owner.GetWithQuery();

    public string BuildExtendWithQuery()
    {
        var dst = Datasource.Destination;

        var cols = new List<string>();
        cols.Add($"{dst.SequenceCommand} as {dst.SequenceKeyColumn}");
        cols.Add($"{this.GetInnerDatasourceAlias()}.*");
        var col = cols.ToString("\r\n, ").AddIndent(4);
        var sql = $@"select
{col}
from {Owner.Alias} {this.GetInnerDatasourceAlias()}
{AdditionalCondition.ToFilter(this).ToWhereSqlText()}";
        sql = $@"{Alias} as (
{sql.AddIndent(4)}
)";
        return sql;
    }
}

