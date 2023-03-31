using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleColumnHelper : IDiscoveredColumnHelper
{
    public static readonly OracleColumnHelper Instance = new();
    private OracleColumnHelper() {}
    public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column, int topX, bool discardNulls)
    {
        var syntax = OracleQuerySyntaxHelper.Instance;

        var sql = new StringBuilder($"SELECT {syntax.EnsureWrapped(column.GetRuntimeName())} FROM {table.GetFullyQualifiedName()}");

        if (discardNulls)
            sql.Append($" WHERE {syntax.EnsureWrapped(column.GetRuntimeName())} IS NOT NULL");

        sql.Append($" OFFSET 0 ROWS FETCH NEXT {topX} ROWS ONLY");
        return sql.ToString();
    }

    public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
    {
        var syntax = column.Table.Database.Server.GetQuerySyntaxHelper();

        var sb = new StringBuilder(
            $"ALTER TABLE {column.Table.GetFullyQualifiedName()} MODIFY {syntax.EnsureWrapped(column.GetRuntimeName())} {newType} ");

        //If you are already null then Oracle will complain (https://www.techonthenet.com/oracle/errors/ora01451.php)
        if (allowNulls != column.AllowNulls)
            sb.Append(allowNulls ? "NULL" : "NOT NULL");

        return  sb.ToString();
    }
}