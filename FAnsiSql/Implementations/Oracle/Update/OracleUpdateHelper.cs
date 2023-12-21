using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.Oracle.Update;

public sealed class OracleUpdateHelper : UpdateHelper
{
    public static readonly OracleUpdateHelper Instance = new();
    private OracleUpdateHelper() {}
    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {

        // This implementation is based on:
        // https://stackoverflow.com/a/32748797/4824531

        /*MERGE INTO table1 t1
USING
(
-- For more complicated queries you can use WITH clause here
SELECT * FROM table2
)t2
ON(t1.id = t2.id)
WHEN MATCHED THEN UPDATE SET
t1.name = t2.name,
t1.desc = t2.desc;*/


        return string.Format(
            """
            MERGE INTO {1} t1
            USING
            (
                SELECT * FROM {2}
            )t2
            on ({3})
            WHEN MATCHED THEN UPDATE SET
                {0}
            WHERE
            {4}
            """,
            string.Join($", {Environment.NewLine}", lines.Where(static l => l.LocationToInsert == QueryComponent.SET).Select(static c => c.Text)),
            table1.GetFullyQualifiedName(),
            table2.GetFullyQualifiedName(),
            string.Join(" AND ", lines.Where(static l => l.LocationToInsert == QueryComponent.JoinInfoJoin).Select(static c => c.Text)),
            string.Join(" AND ", lines.Where(static l => l.LocationToInsert == QueryComponent.WHERE).Select(static c => c.Text)));
    }
}