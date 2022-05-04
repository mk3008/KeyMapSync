using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public class ViewCommand
{
    public string Query { get; set; } = string.Empty;

    public string ViewName { get; set; } = string.Empty;

    public Dictionary<string, object> Parameters { get; set; } = new();

    public void SetParameters(Dictionary<string, object>? prms)
    {
        if (prms == null) return;
        Parameters = prms;
    }

    public SqlCommand ToSqlCommand()
    {
        var sql = $@"create temporary view {ViewName}
as
{Query};";

        return new SqlCommand() { CommandText = sql, Parameters = Parameters };
    }
}