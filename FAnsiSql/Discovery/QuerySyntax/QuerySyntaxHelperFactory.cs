using FAnsi.Implementation;

namespace FAnsi.Discovery.QuerySyntax;

/// <summary>
/// Translates a DatabaseType into the correct IQuerySyntaxHelper.
/// </summary>
public class QuerySyntaxHelperFactory
{
    public IQuerySyntaxHelper Create(DatabaseType type)
    {
        return ImplementationManager.GetImplementation(type).GetQuerySyntaxHelper();
    }
}