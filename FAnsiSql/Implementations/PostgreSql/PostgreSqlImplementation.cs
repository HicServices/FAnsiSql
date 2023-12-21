using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlImplementation() : Implementation<NpgsqlConnectionStringBuilder>(DatabaseType.PostgreSql)
{
    public override IDiscoveredServerHelper GetServerHelper() => PostgreSqlServerHelper.Instance;

    public override bool IsFor(DbConnection connection) => connection is NpgsqlConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => PostgreSqlSyntaxHelper.Instance;
}