using System;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.MySql
{
    public class MySqlColumnHelper : IDiscoveredColumnHelper
    {

        public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column, int topX, bool discardNulls)
        {
            string sql = "SELECT " + column.GetRuntimeName() + " FROM " + table.GetFullyQualifiedName();

            if (discardNulls)
                sql += " WHERE " + column.GetRuntimeName() + " IS NOT NULL";

             sql += " LIMIT "+topX;
            return sql;
        }

        public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
        {
            return "ALTER TABLE " + column.Table.GetRuntimeName() + " MODIFY COLUMN " + column.GetRuntimeName() + " " + newType + " " + (allowNulls ? "NULL" : "NOT NULL");
        }
    }
}