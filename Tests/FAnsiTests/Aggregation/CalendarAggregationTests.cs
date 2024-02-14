using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;

namespace FAnsiTests.Aggregation;

internal sealed class CalendarAggregationTests:AggregationTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_Calendar_Year(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;


        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new(eventDate.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis }, //tell it which the axis are
            new("FROM ", QueryComponent.FROM),
            new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis } //tell it which the axis are
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2010-01-01'",
            AxisIncrement = AxisIncrement.Year //by year
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        TestContext.WriteLine($"About to send SQL:{Environment.NewLine}{sql}");

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(10)); //there are 10 years between 2001 and 2010 even though not all years are represented in the data
        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows[0][0], Is.EqualTo(2001));
            Assert.That(dt.Rows[0][1], Is.EqualTo(5));
            Assert.That(dt.Rows[1][0], Is.EqualTo(2002));
            Assert.That(dt.Rows[1][1], Is.EqualTo(5));
            Assert.That(dt.Rows[2][0], Is.EqualTo(2003));
            Assert.That(dt.Rows[2][1], Is.EqualTo(2));
            Assert.That(dt.Rows[3][0], Is.EqualTo(2004));
            Assert.That(dt.Rows[3][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[4][0], Is.EqualTo(2005));
            Assert.That(dt.Rows[4][1], Is.EqualTo(1));
            Assert.That(dt.Rows[5][0], Is.EqualTo(2006));
            Assert.That(dt.Rows[5][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[6][0], Is.EqualTo(2007));
            Assert.That(dt.Rows[6][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[7][0], Is.EqualTo(2008));
            Assert.That(dt.Rows[7][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[8][0], Is.EqualTo(2009));
            Assert.That(dt.Rows[8][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[9][0], Is.EqualTo(2010));
            Assert.That(dt.Rows[9][1], Is.EqualTo(DBNull.Value));
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_Calendar_Quarter(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var col = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(col.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis },                      //tell it which the axis are
            new("FROM ", QueryComponent.FROM),
            new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(col.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis }                                           //tell it which the axis are
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2010-01-01'",
            AxisIncrement = AxisIncrement.Quarter
        };


        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        ConsoleWriteTable(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(37)); // 4 quarters per year between 2001 and 2009 + 2010Q1

        AssertHasRow(dt, "2001Q1", 5);
        AssertHasRow(dt, "2001Q2", null);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_Calendar_Month(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var syntax = tbl.GetQuerySyntaxHelper();

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new(syntax.EnsureWrapped("EventDate"), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis }, //tell it which the axis are
            new("FROM ", QueryComponent.FROM),
            new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(syntax.EnsureWrapped("EventDate"), QueryComponent.GroupBy) { Role = CustomLineRole.Axis } //tell it which the axis are
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2010-01-01'",
            AxisIncrement = AxisIncrement.Month
        };


        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        ConsoleWriteTable(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(109)); // 109 months between 2001 and 2010 (inclusive)

        AssertHasRow(dt,"2001-01",5);
        AssertHasRow(dt, "2001-02", null);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_Calendar_Day(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var col = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new(col.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis }, //tell it which the axis are
            new("FROM ", QueryComponent.FROM),
            new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(col.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis } //tell it which the axis are
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2010-01-01'",
            AxisIncrement = AxisIncrement.Day
        };


        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        using var con = svr.GetConnection();
        con.Open();

        var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(3288)); // 109 months between 2001 and 2010 (inclusive)

        AssertHasRow(dt, new DateTime(2001,1,1), 4);
        AssertHasRow(dt, new DateTime(2001, 1, 2), 1);
        AssertHasRow(dt, new DateTime(2001, 1, 3), null);
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_Calendar_ToToday(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new(eventDate.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis }, //tell it which the axis are
            new("FROM ", QueryComponent.FROM),
            new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis } //tell it which the axis are
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = tbl.GetQuerySyntaxHelper().GetScalarFunctionSql(MandatoryScalarFunctions.GetTodaysDate),
            AxisIncrement = AxisIncrement.Year //by year
        };


        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        TestContext.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.GreaterThanOrEqualTo(19)); //there are 19 years between 2001 and 2019 (use greater than because we don't want test to fail in 2020)
        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows[0][0], Is.EqualTo(2001));
            Assert.That(dt.Rows[0][1], Is.EqualTo(5));
            Assert.That(dt.Rows[1][0], Is.EqualTo(2002));
            Assert.That(dt.Rows[1][1], Is.EqualTo(5));
            Assert.That(dt.Rows[2][0], Is.EqualTo(2003));
            Assert.That(dt.Rows[2][1], Is.EqualTo(2));
            Assert.That(dt.Rows[3][0], Is.EqualTo(2004));
            Assert.That(dt.Rows[3][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[4][0], Is.EqualTo(2005));
            Assert.That(dt.Rows[4][1], Is.EqualTo(1));
            Assert.That(dt.Rows[5][0], Is.EqualTo(2006));
            Assert.That(dt.Rows[5][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[6][0], Is.EqualTo(2007));
            Assert.That(dt.Rows[6][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[7][0], Is.EqualTo(2008));
            Assert.That(dt.Rows[7][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[8][0], Is.EqualTo(2009));
            Assert.That(dt.Rows[8][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[9][0], Is.EqualTo(2010));
            Assert.That(dt.Rows[9][1], Is.EqualTo(DBNull.Value));

            //should go up to this year
            Assert.That(dt.Rows[^1][0], Is.EqualTo(DateTime.Now.Year));
        });
        Assert.That(dt.Rows[9][1], Is.EqualTo(DBNull.Value));
    }

    /// <summary>
    /// Tests to ensure that the order of the count(*) and EventDate columns don't matter
    /// </summary>
    /// <param name="type"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_Calendar_SELECTColumnOrder_CountAfterAxisColumn(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"{eventDate.GetFullyQualifiedName()},", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.Axis }, //tell it which the axis are
            new("count(*) as MyCount", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new("FROM ", QueryComponent.FROM),
            new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis } //tell it which the axis are
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2010-01-01'",
            AxisIncrement = AxisIncrement.Year //by year
        };


        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        TestContext.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(10)); //there are 10 years between 2001 and 2010 even though not all years are represented in the data
        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows[0][0], Is.EqualTo(2001));
            Assert.That(dt.Rows[0][1], Is.EqualTo(5));
            Assert.That(dt.Rows[1][0], Is.EqualTo(2002));
            Assert.That(dt.Rows[1][1], Is.EqualTo(5));
            Assert.That(dt.Rows[2][0], Is.EqualTo(2003));
            Assert.That(dt.Rows[2][1], Is.EqualTo(2));
            Assert.That(dt.Rows[3][0], Is.EqualTo(2004));
            Assert.That(dt.Rows[3][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[4][0], Is.EqualTo(2005));
            Assert.That(dt.Rows[4][1], Is.EqualTo(1));
            Assert.That(dt.Rows[5][0], Is.EqualTo(2006));
            Assert.That(dt.Rows[5][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[6][0], Is.EqualTo(2007));
            Assert.That(dt.Rows[6][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[7][0], Is.EqualTo(2008));
            Assert.That(dt.Rows[7][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[8][0], Is.EqualTo(2009));
            Assert.That(dt.Rows[8][1], Is.EqualTo(DBNull.Value));
            Assert.That(dt.Rows[9][0], Is.EqualTo(2010));
            Assert.That(dt.Rows[9][1], Is.EqualTo(DBNull.Value));
        });
    }
}