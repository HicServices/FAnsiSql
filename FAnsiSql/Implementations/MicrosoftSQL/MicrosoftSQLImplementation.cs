using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Microsoft.Data.SqlClient;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed class MicrosoftSQLImplementation()
    : Implementation<SqlConnectionStringBuilder>(DatabaseType.MicrosoftSQLServer)
{
    public override IDiscoveredServerHelper GetServerHelper() => MicrosoftSQLServerHelper.Instance;

    public override bool IsFor(DbConnection conn) => conn is SqlConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => MicrosoftQuerySyntaxHelper.Instance;
}