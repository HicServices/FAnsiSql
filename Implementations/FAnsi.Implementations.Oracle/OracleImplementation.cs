using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

public class OracleImplementation : Implementation<OracleConnectionStringBuilder>
{
    public OracleImplementation() : base(DatabaseType.Oracle)
    {
    }

    public override IDiscoveredServerHelper GetServerHelper()
    {
        return new OracleServerHelper();
    }

    public override bool IsFor(DbConnection connection)
    {
        return connection is OracleConnection;
    }

    public override IQuerySyntaxHelper GetQuerySyntaxHelper()
    {
        return new OracleQuerySyntaxHelper();
    }
}