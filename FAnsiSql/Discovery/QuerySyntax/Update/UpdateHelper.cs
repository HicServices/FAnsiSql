using System;
using System.Collections.Generic;
using System.Linq;

namespace FAnsi.Discovery.QuerySyntax.Update;

public abstract class UpdateHelper:IUpdateHelper
{
    /// <summary>
    /// You only have to support CustomLines that fulfil this role in the query i.e. no parameter support etc
    /// </summary>
    private readonly QueryComponent[] _permissableLocations = [QueryComponent.SET, QueryComponent.JoinInfoJoin, QueryComponent.WHERE];

    public string BuildUpdate(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        if(lines.Any(l => !_permissableLocations.Contains(l.LocationToInsert)))
            throw new NotSupportedException();

        return BuildUpdateImpl(table1, table2, lines);
    }

    protected abstract string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines);
}