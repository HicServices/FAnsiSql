using System.Data.Common;
using Microsoft.Data.SqlClient;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;

namespace FAnsi.Implementations.MicrosoftSQL;

public class MicrosoftSQLImplementation : Implementation<SqlConnectionStringBuilder>
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