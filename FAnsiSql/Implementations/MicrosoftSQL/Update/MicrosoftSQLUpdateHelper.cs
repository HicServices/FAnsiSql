﻿using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.MicrosoftSQL.Update;

public class MicrosoftSQLUpdateHelper:UpdateHelper
{
    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        return $"""
                UPDATE t1
                  SET
                    {string.Join($", {Environment.NewLine}", lines.Where(l => l.LocationToInsert == QueryComponent.SET).Select(c => c.Text))}
                  FROM {table1.GetFullyQualifiedName()} AS t1
                  INNER JOIN {table2.GetFullyQualifiedName()} AS t2
                  ON {string.Join(" AND ", lines.Where(l => l.LocationToInsert == QueryComponent.JoinInfoJoin).Select(c => c.Text))}
                WHERE
                {string.Join(" AND ", lines.Where(l => l.LocationToInsert == QueryComponent.WHERE).Select(c => c.Text))}
                """;

    }
}