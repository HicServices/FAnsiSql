using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.PostgreSql.Update;

public class PostgreSqlUpdateHelper : UpdateHelper
{
    public static readonly PostgreSqlUpdateHelper Instance = new();
    private PostgreSqlUpdateHelper(){}

    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        //https://stackoverflow.com/a/7869611
        var joinSql = string.Join(" AND ",
            lines.Where(l => l.LocationToInsert == QueryComponent.JoinInfoJoin).Select(c => c.Text));

        var whereSql = string.Join(" AND ",
            lines.Where(l => l.LocationToInsert == QueryComponent.WHERE).Select(c => c.Text));

        return string.Format(
            """
            UPDATE {1} AS t1
            SET
                {0}
            FROM
             {2} AS t2
            WHERE
            {3}
            {4}
            {5}

            """,

            string.Join($", {Environment.NewLine}",lines.Where(l=>l.LocationToInsert == QueryComponent.SET)
                .Select(c =>
                    //seems like you cant specify the table alias in the SET section of the query
                    c.Text.Replace("t1.",""))),
            table1.GetFullyQualifiedName(),
            table2.GetFullyQualifiedName(),
            joinSql,
            !string.IsNullOrWhiteSpace(whereSql) ? "AND" :"",
            !string.IsNullOrWhiteSpace(whereSql) ? $"({whereSql})" :""
        );

    }
}