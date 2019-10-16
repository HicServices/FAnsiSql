using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlDatabaseHelper : DiscoveredDatabaseHelper
    {
        public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection,
            string database, bool includeViews, DbTransaction transaction = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
            DbConnection connection, string database, DbTransaction transaction = null)
        {
            throw new NotImplementedException();
        }

        public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database)
        {
            throw new NotImplementedException();
        }

        public override IDiscoveredTableHelper GetTableHelper()
        {
            return new PostgreSqlTableHelper();
        }

        public override void DropDatabase(DiscoveredDatabase database)
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
        {
            throw new NotImplementedException();
        }

        public override DirectoryInfo Detach(DiscoveredDatabase database)
        {
            throw new NotImplementedException();
        }

        public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
        {
            throw new NotImplementedException();
        }

        public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
        {
            throw new NotImplementedException();
        }
    }
}