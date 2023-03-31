using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.PostgreSql;

public class PostgreSqlColumnHelper : IDiscoveredColumnHelper
{
    public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table,
        IHasRuntimeName column, int topX,
        bool discardNulls)
    {
        var syntax = new PostgreSqlSyntaxHelper();

        var sql = $"SELECT {syntax.EnsureWrapped(column.GetRuntimeName())} FROM {table.GetFullyQualifiedName()}";

        if (discardNulls)
            sql += $" WHERE {syntax.EnsureWrapped(column.GetRuntimeName())} IS NOT NULL";

        sql += $" fetch first {topX} rows only";
        return sql;
    }

    public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
    {
        var syntax = column.Table.Database.Server.GetQuerySyntaxHelper();

        var sb = new StringBuilder();
        sb.AppendLine(
            $@"ALTER TABLE {column.Table.GetFullyQualifiedName()} ALTER COLUMN {syntax.EnsureWrapped(column.GetRuntimeName())} TYPE {newType};");

        var newNullability = allowNulls ? "NULL" : "NOT NULL";

        if (allowNulls != column.AllowNulls)
            sb.AppendFormat(
                $@"ALTER TABLE {column.Table.GetFullyQualifiedName()} ALTER COLUMN {syntax.EnsureWrapped(column.GetRuntimeName())} SET {newNullability}");
        return sb.ToString();
    }
}