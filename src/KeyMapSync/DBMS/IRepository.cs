using Dapper;
using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.DBMS;

public interface IRepositry
{
    IDBMS Database { get; init; }

    IDbConnection Connection { get; init; }
}

public static class IRepositoryExtension
{
    public static Sequence GetSequence(this IRepositry source, string schema, string table)
    {
        var sql = source.Database.GetSequenceSql();
        return source.Connection.Query<Sequence>(sql, new { schema, table }).First();
    }

    public static List<string> GetColumns(this IRepositry source, string schema, string table)
    {
        var sql = source.Database.GetColumnsSql();
        return source.Connection.Query<string>(sql, new { schema, table }).ToList();
    }

    public static Dictionary<string, DbColumn.Types> GetKeyColumns(this IRepositry source, string? schema, string table)
    {
        var q = source.Connection.Query(source.Database.GetKeyColumnsSql(), new { schema, table }).ToList();
        var dic = new Dictionary<string, DbColumn.Types>();

        q.ForEach(x =>
        {
            if (x.data_type == "smallint") dic[x.column_name] = DbColumn.Types.Numeric;
            else if (x.data_type == "int2") dic[x.column_name] = DbColumn.Types.Numeric;

            else if (x.data_type == "integer") dic[x.column_name] = DbColumn.Types.Numeric;
            else if (x.data_type == "int") dic[x.column_name] = DbColumn.Types.Numeric;
            else if (x.data_type == "int4") dic[x.column_name] = DbColumn.Types.Numeric;

            else if (x.data_type == "bigint") dic[x.column_name] = DbColumn.Types.Numeric;
            else if (x.data_type == "int8") dic[x.column_name] = DbColumn.Types.Numeric;

            else if (x.data_type == "serial") dic[x.column_name] = DbColumn.Types.Numeric;
            else if (x.data_type == "serial4") dic[x.column_name] = DbColumn.Types.Numeric;

            else if (x.data_type == "bigserial") dic[x.column_name] = DbColumn.Types.Numeric;
            else if (x.data_type == "serial8") dic[x.column_name] = DbColumn.Types.Numeric;

            else if (x.data_type == "date") dic[x.column_name] = DbColumn.Types.Date;

            else if (x.data_type == "timestamp") dic[x.column_name] = DbColumn.Types.Timestamp;

            else dic[x.column_name] = DbColumn.Types.Text;
        });

        return dic;
    }
}