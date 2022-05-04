using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace KeyMapSync.Database;

public partial class DbExecutor
{
    public static event EventHandler<SqlEventArgs> OnBeforeExecute;

    public static event EventHandler<SqlResultArgs> OnAfterExecute;

    public int? CommandTimeout { get; set; }

    public DbExecutor(IDBMS db, IDbConnection cn)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(cn);

        DB = db;
        Connection = cn;
    }

    public IDBMS DB { get; private set; }

    public IDbConnection Connection { get; private set; }
}
