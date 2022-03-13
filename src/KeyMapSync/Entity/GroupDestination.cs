using KeyMapSync.DMBS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Entity;

public class GroupDestination
{
    /// <summary>
    /// ex.integration_sale
    /// </summary>
    public string DestinationTableName { get; set; } = string.Empty;

    public Sequence Sequence { get; set; } = new();

    /// <summary>
    /// 
    /// </summary>
    public List<string> Columns { get; init; } = new();

    public string GetInnerAlias() => $"g_{DestinationTableName}".ToLower(); 

    public List<string> GetColumnsWithoutKey() => Columns.Where(x => x != Sequence.Column).ToList();

    public TablePair ToTablePair(Datasource d)
    {
        var pair = new TablePair()
        {
            FromTable = d.BridgeName,
            ToTable = DestinationTableName
        };

        pair.AddColumnPair(Sequence.Column);
        GetColumnsWithoutKey().Where(x => d.Columns.Contains(x)).ToList().ForEach(x => pair.AddColumnPair(x));

        return pair;
    }

    public SqlCommand ToInsertCommand(Datasource d)
    {
        var p = ToTablePair(d);
        var ic = p.ToInsertCommand();
        ic.SelectSql.UseDistinct = true;
        var cmd = ic.ToSqlCommand();

        return cmd;
    }
}

