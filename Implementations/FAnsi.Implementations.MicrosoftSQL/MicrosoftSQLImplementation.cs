using System.Data.Common;
using Microsoft.Data.SqlClient;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed class MicrosoftSQLImplementation : Implementation<SqlConnectionStringBuilder>
{
    public MicrosoftSQLImplementation():base(DatabaseType.MicrosoftSQLServer)
    {
            
    }

    public override IDiscoveredServerHelper GetServerHelper() => MicrosoftSQLServerHelper.Instance;

    public override bool IsFor(DbConnection conn) => conn is SqlConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => MicrosoftQuerySyntaxHelper.Instance;
}