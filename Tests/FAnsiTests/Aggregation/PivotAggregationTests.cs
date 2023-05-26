using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System.Collections.Generic;
using System.Data;

namespace FAnsiTests.Aggregation;

internal class PivotAggregationTests:AggregationTests
{
    [TestCase(DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MySql)]
    public void Test_PivotOnlyCount(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("Category as Cat,", QueryComponent.QueryTimeColumn),
            new("EventDate as Ev", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new("Category,", QueryComponent.GroupBy),
            new("EventDate", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();
        //Expected Test Results:
        /*
                Cat	    2001-01-01 00:00:00.0000000	2002-01-01 00:00:00.0000000	2002-02-01 00:00:00.0000000	2002-03-02 00:00:00.0000000	2003-01-01 00:00:00.0000000	2003-04-02 00:00:00.0000000	2005-01-01 00:00:00.0000000	2001-01-02 00:00:00.0000000
                E&, %a' mp;E	1	1	0	0	0	0	1	0
                F	            0	2	0	0	0	0	0	0
                G	            1	0	0	0	0	0	0	0
                T	            2	0	1	1	1	1	0	1
             */

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.AreEqual(9, dt.Columns.Count);
        Assert.AreEqual(4, dt.Rows.Count);
        Assert.AreEqual("Cat", dt.Columns[0].ColumnName);
    }
}