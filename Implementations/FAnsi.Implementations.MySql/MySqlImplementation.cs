using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using MySqlConnector;

namespace FAnsi.Implementations.MySql
{
    public class MySqlImplementation : Implementation<MySqlConnectionStringBuilder>
    {
        public MySqlImplementation(): base(DatabaseType.MySql)
        {
        }

        public override IDiscoveredServerHelper GetServerHelper()
        {
            return new MySqlServerHelper();
        }

        public override bool IsFor(DbConnection connection)
        {
            return connection is MySqlConnection;
        }

        public override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new MySqlQuerySyntaxHelper();
        }
    }
}