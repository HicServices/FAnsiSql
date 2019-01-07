using System.ComponentModel.Composition;
using System.Data.Common;
using System.Data.SqlClient;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;

namespace FAnsiSql.Implementations.MicrosoftSQL
{
    class MicrosoftSQLImplementation : Implementation<SqlConnectionStringBuilder>
    {
        public MicrosoftSQLImplementation():base(DatabaseType.MicrosoftSQLServer)
        {
            
        }

        public override IDiscoveredServerHelper GetServerHelper()
        {
            return new MicrosoftSQLServerHelper();
        }

        public override bool IsFor(DbConnection conn)
        {
            return conn is SqlConnection;
        }

        public override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new MicrosoftQuerySyntaxHelper();
        }
    }
}
