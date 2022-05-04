using KeyMapSync.DBMS;
using KeyMapSync.DBMS;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public static class IAbutmentSqlExtension
{
    public static SqlCommand ToTemporaryViewDdl(this IAbutment source)
    {
        var cmd = new ViewCommand()
        {
            Query = source.Datasource.Query, 
            ViewName = source.ViewName, 
        };

        return cmd.ToSqlCommand();
    }
}

