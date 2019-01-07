using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementation
{
    public interface IImplementation
    {
        DbConnectionStringBuilder GetBuilder();
        IDiscoveredServerHelper GetServerHelper();

        bool IsFor(DatabaseType databaseType);
        bool IsFor(DbConnectionStringBuilder builder);
        bool IsFor(DbConnection connection);

        IQuerySyntaxHelper GetQuerySyntaxHelper();
    }
}