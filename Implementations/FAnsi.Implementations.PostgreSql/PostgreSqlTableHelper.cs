using System;
using System.Data.Common;
using System.Globalization;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlTableHelper : DiscoveredTableHelper
    {
        public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX)
        {
            throw new NotImplementedException();
        }

        public override DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
        {
            throw new NotImplementedException();
        }

        public override IDiscoveredColumnHelper GetColumnHelper()
        {
            return new PostgreSqlColumnHelper();
        }

        public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
        {
            throw new NotImplementedException();
        }

        public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
        {
            throw new NotImplementedException();
        }

        public override DiscoveredParameter[] DiscoverTableValuedFunctionParameters(DbConnection connection,
            DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture)
        {
            return new PostgreSqlBulkCopy(discoveredTable, connection,culture);
        }

        public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection,
            IManagedTransaction transaction = null)
        {
            throw new NotImplementedException();
        }

        protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
        {
            throw new NotImplementedException();
        }
    }
}