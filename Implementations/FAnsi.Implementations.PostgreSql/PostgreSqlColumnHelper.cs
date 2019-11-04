using System;
using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlColumnHelper : IDiscoveredColumnHelper
    {
        public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table,
            IHasRuntimeName column, int topX,
            bool discardNulls)
        {
            string sql = "SELECT \"" + column.GetRuntimeName() + "\" FROM " + table.GetFullyQualifiedName();

            if (discardNulls)
                sql += " WHERE \"" + column.GetRuntimeName() + "\" IS NOT NULL";

            sql += " fetch first " + topX + " rows only";
            return sql;
        }

        public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine(
                $@"ALTER TABLE ""{column.Table.GetRuntimeName()}"" ALTER COLUMN ""{column.GetRuntimeName()}"" TYPE {newType};");

            var newNullability = allowNulls ? "NULL" : "NOT NULL";

            if (allowNulls != column.AllowNulls)
                sb.AppendFormat(
                    $@"ALTER TABLE ""{column.Table.GetRuntimeName()}"" ALTER COLUMN ""{column.GetRuntimeName()}"" SET {newNullability}");
            return sb.ToString();
        }
    }
}