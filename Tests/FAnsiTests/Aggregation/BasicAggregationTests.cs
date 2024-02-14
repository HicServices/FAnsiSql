using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;

namespace FAnsiTests.Aggregation;

internal sealed class BasicAggregationTests : AggregationTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_BasicCount(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*)", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        Assert.That(Convert.ToInt32(cmd.ExecuteScalar()), Is.EqualTo(14));
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_GroupByCount(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*),", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(category.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("ORDER BY", QueryComponent.OrderBy),
            new(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };


        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows, Has.Count.EqualTo(4));

            Assert.That(dt.Rows[0][1], Is.EqualTo("E&, %a' mp;E"));
            Assert.That(dt.Rows[0][0], Is.EqualTo(3));

            Assert.That(dt.Rows[1][1], Is.EqualTo("F"));
            Assert.That(dt.Rows[1][0], Is.EqualTo(2));
        });
    }
}