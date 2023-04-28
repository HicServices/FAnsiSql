using System.ComponentModel.Composition;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementation;

[InheritedExport(typeof(IImplementation))]
public abstract class Implementation<T> : IImplementation where T:DbConnectionStringBuilder,new()
{
    private readonly DatabaseType _databaseType;

    protected Implementation(DatabaseType databaseType)
    {
        _databaseType = databaseType;
    }

    public virtual DbConnectionStringBuilder GetBuilder()
    {
        return new T();
    }

    public abstract IDiscoveredServerHelper GetServerHelper();

    public virtual bool IsFor(DatabaseType databaseType)
    {
        return _databaseType == databaseType;
    }


    public virtual bool IsFor(DbConnectionStringBuilder builder)
    {
        return builder is T;
    }

    public abstract bool IsFor(DbConnection connection);

    public abstract IQuerySyntaxHelper GetQuerySyntaxHelper();
}