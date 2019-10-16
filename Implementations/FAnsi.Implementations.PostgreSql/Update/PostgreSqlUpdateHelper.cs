using System.Collections.Generic;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.PostgreSql.Update
{
    public class PostgreSqlUpdateHelper : UpdateHelper
    {
        protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
        {
            throw new System.NotImplementedException();
        }
    }
}