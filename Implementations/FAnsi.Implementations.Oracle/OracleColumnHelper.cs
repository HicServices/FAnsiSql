using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.Oracle
{
    public class OracleColumnHelper : IDiscoveredColumnHelper
    {
        public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column, int topX, bool discardNulls)
        {
           string sql = "SELECT " + column.GetRuntimeName() + " FROM " + table.GetFullyQualifiedName();

            if (discardNulls)
                sql += " WHERE " + column.GetRuntimeName() + " IS NOT NULL";

             sql += " OFFSET 0 ROWS FETCH NEXT "+topX+" ROWS ONLY";
            return sql;
        }

        public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
        {
            StringBuilder sb = new StringBuilder("ALTER TABLE " + column.Table.GetFullyQualifiedName() + " MODIFY " + column.GetRuntimeName() + " " + newType + " ");

            //If you are already null then Oracle will complain (https://www.techonthenet.com/oracle/errors/ora01451.php)
            if (allowNulls != column.AllowNulls)
                sb.Append(allowNulls ? "NULL" : "NOT NULL");

            return  sb.ToString();
        }
    }
}