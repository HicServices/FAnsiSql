using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.MySql;

public class MySqlColumnHelper : IDiscoveredColumnHelper
{

    public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column, int topX, bool discardNulls)
    {
        var syntax = new MySqlQuerySyntaxHelper();

        string sql = "SELECT " + syntax.EnsureWrapped(column.GetRuntimeName()) + " FROM " + table.GetFullyQualifiedName();

        if (discardNulls)
            sql += " WHERE " + syntax.EnsureWrapped(column.GetRuntimeName()) + " IS NOT NULL";

        sql += " LIMIT "+topX;
        return sql;
    }

    public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
    {
        var syntax = column.Table.Database.Server.GetQuerySyntaxHelper();
        return "ALTER TABLE " + column.Table.GetFullyQualifiedName() + " MODIFY COLUMN " + syntax.EnsureWrapped(column.GetRuntimeName()) + " " + newType + " " + (allowNulls ? "NULL" : "NOT NULL");
    }
}