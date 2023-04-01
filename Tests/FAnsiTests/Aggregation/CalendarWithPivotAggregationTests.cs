using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;

namespace FAnsiTests.Aggregation;

internal class CalendarWithPivotAggregationTests:AggregationTests
{
    [TestCase(DatabaseType.MicrosoftSQLServer,true)]
    [TestCase(DatabaseType.MySql,true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, false)]
    [TestCase(DatabaseType.MySql, false)]
    public void Test_Calendar_WithPivot(DatabaseType type,bool easy)
    {
        string sql=null!;
        try
        {
            var tbl = GetTestTable(type,easy);
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>
            {
                new("SELECT", QueryComponent.SELECT),
                new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                    { Role = CustomLineRole.CountFunction },
                new("EventDate,", QueryComponent.QueryTimeColumn)
                    { Role = CustomLineRole.Axis }, //tell it which the axis are 
                new("Category", QueryComponent.QueryTimeColumn)
                    { Role = CustomLineRole.Pivot }, //tell it which the pivot
                new("FROM ", QueryComponent.FROM),
                new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
                new("GROUP BY", QueryComponent.GroupBy),
                new("EventDate,", QueryComponent.GroupBy)
                    { Role = CustomLineRole.Axis }, //tell it which the axis are 
                new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
            };

            var axis = new QueryAxis
            {
                StartDate = "'2001-01-01'",
                EndDate = "'2010-01-01'",
                AxisIncrement = AxisIncrement.Year //by year
            };


            sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

            using var con = svr.GetConnection();
            con.Open();

            var da = svr.GetDataAdapter(sql, con);
            using var dt = new DataTable();
            da.Fill(dt);

            //pivot columns should ordered by sum of pivot values (T has the highest followed by E...)

            /*joinDt	T	E&, %a' mp;E	F	G
                    2001	3	1	            0	1
                    2002	2	1	            2	0
                    2003	2	0	            0	0
                    2004	0	0	            0	0
                    2005	0	1	            0	0
                    2006	0	0	            0	0
                    2007	0	0	            0	0
                    2008	0	0	            0	0
                    2009	0	0	            0	0
                    2010	0	0	            0	0
*/

            Assert.AreEqual(10,
                dt.Rows.Count); //there are 10 years between 2001 and 2010 even though not all years are represented in the data

            // only validate hard output, we got rows on easy thats enough for now
            if (easy)
                return;

            StringAssert.AreEqualIgnoringCase("joinDt", dt.Columns[0].ColumnName);
            StringAssert.AreEqualIgnoringCase("T", dt.Columns[1].ColumnName);
            StringAssert.AreEqualIgnoringCase("E&, %a' mp;E", dt.Columns[2].ColumnName);
            StringAssert.AreEqualIgnoringCase("F", dt.Columns[3].ColumnName);
            StringAssert.AreEqualIgnoringCase("G", dt.Columns[4].ColumnName);

            Assert.AreEqual(2001, dt.Rows[0][0]);
            Assert.AreEqual(3, dt.Rows[0][1]);
            Assert.AreEqual(1, dt.Rows[0][2]);
            Assert.AreEqual(0, dt.Rows[0][3]);
            Assert.AreEqual(1, dt.Rows[0][4]);

            Assert.AreEqual(2002, dt.Rows[1][0]);
            Assert.AreEqual(2, dt.Rows[1][1]);
            Assert.AreEqual(1, dt.Rows[1][2]);
            Assert.AreEqual(2, dt.Rows[1][3]);
            Assert.AreEqual(0, dt.Rows[1][4]);

            Assert.AreEqual(2003, dt.Rows[2][0]);
            Assert.AreEqual(2, dt.Rows[2][1]);
            Assert.AreEqual(0, dt.Rows[2][2]);
            Assert.AreEqual(0, dt.Rows[2][3]);
            Assert.AreEqual(0, dt.Rows[2][4]);

            Assert.AreEqual(2004, dt.Rows[3][0]);
            Assert.AreEqual(0, dt.Rows[3][1] == DBNull.Value ? 0 : dt.Rows[3][1]);
            Assert.AreEqual(0,
                dt.Rows[3][2] == DBNull.Value
                    ? 0
                    : dt.Rows[3][
                        1]); //null is permitted because this row doesn't have any matching records... peculiarity of MySql implementation but null=0 is ok for aggregates
            Assert.AreEqual(0, dt.Rows[3][3] == DBNull.Value ? 0 : dt.Rows[3][1]);
            Assert.AreEqual(0, dt.Rows[3][4] == DBNull.Value ? 0 : dt.Rows[3][1]);

            Assert.AreEqual(2005, dt.Rows[4][0]);
            Assert.AreEqual(0, dt.Rows[4][1]);
            Assert.AreEqual(1, dt.Rows[4][2]);
            Assert.AreEqual(0, dt.Rows[4][3]);
            Assert.AreEqual(0, dt.Rows[4][4]);
        }
        catch (Exception)
        {
            TestContext.Error.WriteLine($"SQL triggering error was: '{sql ?? "None defined"}'");
            throw;
        }
    }
}