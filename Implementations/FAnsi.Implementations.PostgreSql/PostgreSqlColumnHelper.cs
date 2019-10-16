using System;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlColumnHelper : IDiscoveredColumnHelper
    {
        public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column, int topX,
            bool discardNulls)
        {
            throw new NotImplementedException();
        }

        public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
        {
            throw new NotImplementedException();
        }
    }
}