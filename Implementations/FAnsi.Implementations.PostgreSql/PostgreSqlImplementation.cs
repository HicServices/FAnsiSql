using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlImplementation : Implementation<NpgsqlConnectionStringBuilder>
    {
        public PostgreSqlImplementation() : base(DatabaseType.PostgreSql)
        {
        }

        public override IDiscoveredServerHelper GetServerHelper()
        {
            return new PostgreSqlServerHelper();
        }

        public override bool IsFor(DbConnection connection)
        {
            return connection is NpgsqlConnection;
        }

        public override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new PostgreSqlSyntaxHelper();
        }
    }
}
