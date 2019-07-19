using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace FAnsiTests.Aggregation
{
    class CalendarAggregationTests:AggregationTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void Test_Calendar_Year(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();
            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            lines.Add(new CustomLine("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("EventDate", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis });                      //tell it which the axis are 
            lines.Add(new CustomLine("FROM ", QueryComponent.FROM));
            lines.Add(new CustomLine(tbl.GetFullyQualifiedName(), QueryComponent.FROM));
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate", QueryComponent.GroupBy) { Role = CustomLineRole.Axis });                                           //tell it which the axis are 

            var axis = new QueryAxis()
            {
                StartDate = "'2001-01-01'",
                EndDate = "'2010-01-01'",
                AxisIncrement = AxisIncrement.Year //by year
            };


            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis, false);

            using (var con = svr.GetConnection())
            {
                con.Open();

                var da = svr.GetDataAdapter(sql, con);
                DataTable dt = new DataTable();
                da.Fill(dt);

                Assert.AreEqual(10, dt.Rows.Count); //there are 10 years between 2001 and 2010 even though not all years are represented in the data
                Assert.AreEqual(2001, dt.Rows[0][0]);
                Assert.AreEqual(5, dt.Rows[0][1]);
                Assert.AreEqual(2002, dt.Rows[1][0]);
                Assert.AreEqual(5, dt.Rows[1][1]);
                Assert.AreEqual(2003, dt.Rows[2][0]);
                Assert.AreEqual(2, dt.Rows[2][1]);
                Assert.AreEqual(2004, dt.Rows[3][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[3][1]);
                Assert.AreEqual(2005, dt.Rows[4][0]);
                Assert.AreEqual(1, dt.Rows[4][1]);
                Assert.AreEqual(2006, dt.Rows[5][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[5][1]);
                Assert.AreEqual(2007, dt.Rows[6][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[6][1]);
                Assert.AreEqual(2008, dt.Rows[7][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[7][1]);
                Assert.AreEqual(2009, dt.Rows[8][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[8][1]);
                Assert.AreEqual(2010, dt.Rows[9][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[9][1]);
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void Test_Calendar_Month(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();
            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            lines.Add(new CustomLine("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("EventDate", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis });                      //tell it which the axis are 
            lines.Add(new CustomLine("FROM ", QueryComponent.FROM));
            lines.Add(new CustomLine(tbl.GetFullyQualifiedName(), QueryComponent.FROM));
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate", QueryComponent.GroupBy) { Role = CustomLineRole.Axis });                                           //tell it which the axis are 

            var axis = new QueryAxis()
            {
                StartDate = "'2001-01-01'",
                EndDate = "'2010-01-01'",
                AxisIncrement = AxisIncrement.Month
            };


            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis, false);

            using (var con = svr.GetConnection())
            {
                con.Open();

                var da = svr.GetDataAdapter(sql, con);
                DataTable dt = new DataTable();
                da.Fill(dt);

                ConsoleWriteTable(dt);

                Assert.AreEqual(109, dt.Rows.Count); // 109 months between 2001 and 2010 (inclusive)

                AssertHasRow(dt,"2001-01",5);
                AssertHasRow(dt, "2001-02", null);
            }
        }


        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void Test_Calendar_ToToday(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();
            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            lines.Add(new CustomLine("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("EventDate", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis });                      //tell it which the axis are 
            lines.Add(new CustomLine("FROM ", QueryComponent.FROM));
            lines.Add(new CustomLine(tbl.GetFullyQualifiedName(), QueryComponent.FROM));
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate", QueryComponent.GroupBy) { Role = CustomLineRole.Axis });                                           //tell it which the axis are 

            var axis = new QueryAxis()
            {
                StartDate = "'2001-01-01'",
                EndDate = tbl.GetQuerySyntaxHelper().GetScalarFunctionSql(MandatoryScalarFunctions.GetTodaysDate),
                AxisIncrement = AxisIncrement.Year //by year
            };


            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis, false);

            using (var con = svr.GetConnection())
            {
                con.Open();

                var da = svr.GetDataAdapter(sql, con);
                DataTable dt = new DataTable();
                da.Fill(dt);

                Assert.GreaterOrEqual(dt.Rows.Count, 19); //there are 19 years between 2001 and 2019 (use greater than because we don't want test to fail in 2020)
                Assert.AreEqual(2001, dt.Rows[0][0]);
                Assert.AreEqual(5, dt.Rows[0][1]);
                Assert.AreEqual(2002, dt.Rows[1][0]);
                Assert.AreEqual(5, dt.Rows[1][1]);
                Assert.AreEqual(2003, dt.Rows[2][0]);
                Assert.AreEqual(2, dt.Rows[2][1]);
                Assert.AreEqual(2004, dt.Rows[3][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[3][1]);
                Assert.AreEqual(2005, dt.Rows[4][0]);
                Assert.AreEqual(1, dt.Rows[4][1]);
                Assert.AreEqual(2006, dt.Rows[5][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[5][1]);
                Assert.AreEqual(2007, dt.Rows[6][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[6][1]);
                Assert.AreEqual(2008, dt.Rows[7][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[7][1]);
                Assert.AreEqual(2009, dt.Rows[8][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[8][1]);
                Assert.AreEqual(2010, dt.Rows[9][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[9][1]);

                //should go up to this year
                Assert.AreEqual(DateTime.Now.Year, dt.Rows[dt.Rows.Count - 1][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[9][1]);
            }
        }

        /// <summary>
        /// Tests to ensure that the order of the count(*) and EventDate columns don't matter
        /// </summary>
        /// <param name="type"></param>
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void Test_Calendar_SELECTColumnOrder_CountAfterAxisColumn(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();
            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            lines.Add(new CustomLine("EventDate,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis });                      //tell it which the axis are 
            lines.Add(new CustomLine("count(*) as MyCount", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("FROM ", QueryComponent.FROM));
            lines.Add(new CustomLine(tbl.GetFullyQualifiedName(), QueryComponent.FROM));
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate", QueryComponent.GroupBy) { Role = CustomLineRole.Axis });                                           //tell it which the axis are 

            var axis = new QueryAxis()
            {
                StartDate = "'2001-01-01'",
                EndDate = "'2010-01-01'",
                AxisIncrement = AxisIncrement.Year //by year
            };


            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis, false);

            using (var con = svr.GetConnection())
            {
                con.Open();

                var da = svr.GetDataAdapter(sql, con);
                DataTable dt = new DataTable();
                da.Fill(dt);

                Assert.AreEqual(10, dt.Rows.Count); //there are 10 years between 2001 and 2010 even though not all years are represented in the data
                Assert.AreEqual(2001, dt.Rows[0][0]);
                Assert.AreEqual(5, dt.Rows[0][1]);
                Assert.AreEqual(2002, dt.Rows[1][0]);
                Assert.AreEqual(5, dt.Rows[1][1]);
                Assert.AreEqual(2003, dt.Rows[2][0]);
                Assert.AreEqual(2, dt.Rows[2][1]);
                Assert.AreEqual(2004, dt.Rows[3][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[3][1]);
                Assert.AreEqual(2005, dt.Rows[4][0]);
                Assert.AreEqual(1, dt.Rows[4][1]);
                Assert.AreEqual(2006, dt.Rows[5][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[5][1]);
                Assert.AreEqual(2007, dt.Rows[6][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[6][1]);
                Assert.AreEqual(2008, dt.Rows[7][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[7][1]);
                Assert.AreEqual(2009, dt.Rows[8][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[8][1]);
                Assert.AreEqual(2010, dt.Rows[9][0]);
                Assert.AreEqual(DBNull.Value, dt.Rows[9][1]);
            }
        }
    }
}
