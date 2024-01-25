using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementation;

public abstract class Implementation<T>(DatabaseType type) : IImplementation
    where T : DbConnectionStringBuilder, new()
{
    public virtual DbConnectionStringBuilder GetBuilder() => new T();

    public abstract IDiscoveredServerHelper GetServerHelper();

    public virtual bool IsFor(DatabaseType databaseType) => type == databaseType;

    public virtual bool IsFor(DbConnectionStringBuilder builder) => builder is T;

    public abstract bool IsFor(DbConnection connection);

    public abstract IQuerySyntaxHelper GetQuerySyntaxHelper();
}