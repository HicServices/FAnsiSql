using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;

namespace FAnsiTests.Aggregation;

internal class BasicAggregationTests : AggregationTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_BasicCount(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new CustomLine("SELECT", QueryComponent.SELECT),
            new CustomLine("count(*)", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new CustomLine($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        Assert.AreEqual(14, Convert.ToInt32(cmd.ExecuteScalar()));
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_GroupByCount(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");

        var lines = new List<CustomLine>
        {
            new CustomLine("SELECT", QueryComponent.SELECT),
            new CustomLine("count(*),", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new CustomLine(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new CustomLine($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new CustomLine("GROUP BY", QueryComponent.GroupBy),
            new CustomLine(category.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new CustomLine("ORDER BY", QueryComponent.OrderBy),
            new CustomLine(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };


        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.AreEqual(4, dt.Rows.Count);
        Assert.AreEqual("E&, %a' mp;E", dt.Rows[0][1]);
        Assert.AreEqual(3, dt.Rows[0][0]);

        Assert.AreEqual("F", dt.Rows[1][1]);
        Assert.AreEqual(2, dt.Rows[1][0]);
    }
}