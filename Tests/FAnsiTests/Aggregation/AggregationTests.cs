using System;
using System.Collections.Generic;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;

namespace FAnsiTests.Aggregation
{
    class AggregationTests:DatabaseTests
    {
        private readonly Dictionary<DatabaseType, DiscoveredTable> _testTables = new Dictionary<DatabaseType, DiscoveredTable>();
            
        [OneTimeSetUp]
        public void Setup()
        {
            DataTable dt = new DataTable();
            dt.TableName = "AggregateDataBasedTests";

            dt.Columns.Add("EventDate");
            dt.Columns.Add("Category");
            dt.Columns.Add("NumberInTrouble");

            dt.Rows.Add("2001-01-01", "T", "7");
            dt.Rows.Add("2001-01-02", "T", "11");
            dt.Rows.Add("2001-01-01", "T", "49");

            dt.Rows.Add("2002-02-01", "T", "13");
            dt.Rows.Add("2002-03-02", "T", "17");
            dt.Rows.Add("2003-01-01", "T", "19");
            dt.Rows.Add("2003-04-02", "T", "23");


            dt.Rows.Add("2002-01-01", "F", "29");
            dt.Rows.Add("2002-01-01", "F", "31");

            dt.Rows.Add("2001-01-01", "E&, %a' mp;E", "37");
            dt.Rows.Add("2002-01-01", "E&, %a' mp;E", "41");
            dt.Rows.Add("2005-01-01", "E&, %a' mp;E", "59");  //note there are no records in 2004 it is important for axis tests (axis involves you having to build a calendar table)

            dt.Rows.Add(null, "G", "47");
            dt.Rows.Add("2001-01-01", "G", "53");

            
            foreach (KeyValuePair<DatabaseType, string> kvp in TestConnectionStrings)
            {
                
                var db = GetTestDatabase(kvp.Key);
                var tbl = db.CreateTable("AggregateDataBasedTests", dt);
                _testTables.Add(kvp.Key,tbl);
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        public void Test_BasicCount(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();

            lines.Add(new CustomLine("SELECT",QueryComponent.SELECT));
            lines.Add(new CustomLine("count(*)", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("FROM " + tbl.GetFullyQualifiedName(),QueryComponent.FROM));

            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines,null,false);

            using (var con = svr.GetConnection())
            {
                con.Open();

                var cmd = svr.GetCommand(sql, con);
                Assert.AreEqual(14,Convert.ToInt32(cmd.ExecuteScalar()));
            }
        }


        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        public void Test_GroupByCount(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();

            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            
            lines.Add(new CustomLine("count(*),", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("Category", QueryComponent.QueryTimeColumn));
            
            lines.Add(new CustomLine("FROM " + tbl.GetFullyQualifiedName(), QueryComponent.FROM));
            
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("Category", QueryComponent.GroupBy));

            lines.Add(new CustomLine("ORDER BY", QueryComponent.OrderBy));
            lines.Add(new CustomLine("Category", QueryComponent.OrderBy));


            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null, false);

            using (var con = svr.GetConnection())
            {
                con.Open();

                var da = svr.GetDataAdapter(sql, con);
                DataTable dt = new DataTable();
                da.Fill(dt);

                Assert.AreEqual(4,dt.Rows.Count);
                Assert.AreEqual("E&, %a' mp;E", dt.Rows[0][1]);
                Assert.AreEqual(3, dt.Rows[0][0]);

                Assert.AreEqual("F", dt.Rows[1][1]);
                Assert.AreEqual(2, dt.Rows[1][0]);
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        public void Test_Calendar(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();
            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            lines.Add(new CustomLine("count(*) as MyCount,", QueryComponent.QueryTimeColumn){Role = CustomLineRole.CountFunction});
            lines.Add(new CustomLine("EventDate", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis });                      //tell it which the axis are 
            lines.Add(new CustomLine("FROM ", QueryComponent.FROM));
            lines.Add(new CustomLine(tbl.GetFullyQualifiedName(),QueryComponent.FROM));
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate", QueryComponent.GroupBy){Role = CustomLineRole.Axis});                                           //tell it which the axis are 
            
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
                Assert.AreEqual(2001,           dt.Rows[0][0]);
                Assert.AreEqual(5,              dt.Rows[0][1]);
                Assert.AreEqual(2002,           dt.Rows[1][0]);
                Assert.AreEqual(5,              dt.Rows[1][1]);
                Assert.AreEqual(2003,           dt.Rows[2][0]);
                Assert.AreEqual(2,              dt.Rows[2][1]);
                Assert.AreEqual(2004,           dt.Rows[3][0]);
                Assert.AreEqual(DBNull.Value,   dt.Rows[3][1]);
                Assert.AreEqual(2005,           dt.Rows[4][0]);
                Assert.AreEqual(1,              dt.Rows[4][1]);
                Assert.AreEqual(2006,           dt.Rows[5][0]);
                Assert.AreEqual(DBNull.Value,   dt.Rows[5][1]);
                Assert.AreEqual(2007,           dt.Rows[6][0]);
                Assert.AreEqual(DBNull.Value,   dt.Rows[6][1]);
                Assert.AreEqual(2008,           dt.Rows[7][0]);
                Assert.AreEqual(DBNull.Value,   dt.Rows[7][1]);
                Assert.AreEqual(2009,           dt.Rows[8][0]);
                Assert.AreEqual(DBNull.Value,   dt.Rows[8][1]);
                Assert.AreEqual(2010,           dt.Rows[9][0]);
                Assert.AreEqual(DBNull.Value,   dt.Rows[9][1]);
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        public void Test_Calendar_WithPivot(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();
            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            lines.Add(new CustomLine("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("EventDate,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis });                      //tell it which the axis are 
            lines.Add(new CustomLine("Category", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot });                      //tell it which the pivot
            lines.Add(new CustomLine("FROM ", QueryComponent.FROM));
            lines.Add(new CustomLine(tbl.GetFullyQualifiedName(), QueryComponent.FROM));
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate,", QueryComponent.GroupBy) { Role = CustomLineRole.Axis });                                           //tell it which the axis are 
            lines.Add(new CustomLine("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot });

            var axis = new QueryAxis()
            {
                StartDate = "'2001-01-01'",
                EndDate = "'2010-01-01'",
                AxisIncrement = AxisIncrement.Year //by year
            };


            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis, true);

            using (var con = svr.GetConnection())
            {
                con.Open();

                var da = svr.GetDataAdapter(sql, con);
                DataTable dt = new DataTable();
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

                Assert.AreEqual(10, dt.Rows.Count); //there are 10 years between 2001 and 2010 even though not all years are represented in the data

                StringAssert.AreEqualIgnoringCase("joinDt",dt.Columns[0].ColumnName);
                StringAssert.AreEqualIgnoringCase("T", dt.Columns[1].ColumnName);
                StringAssert.AreEqualIgnoringCase("E&, %a' mp;E", dt.Columns[2].ColumnName);
                StringAssert.AreEqualIgnoringCase("F", dt.Columns[3].ColumnName);
                StringAssert.AreEqualIgnoringCase("G", dt.Columns[4].ColumnName);

                Assert.AreEqual(2001, dt.Rows[0][0]);
                Assert.AreEqual(3 , dt.Rows[0][1]);
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
                Assert.AreEqual(0, dt.Rows[3][2] == DBNull.Value ? 0 : dt.Rows[3][1]);  //null is permitted because this row doesn't have any matching records... peculiarity of MySql implementation but null=0 is ok for aggregates
                Assert.AreEqual(0, dt.Rows[3][3] == DBNull.Value ? 0 : dt.Rows[3][1]);
                Assert.AreEqual(0, dt.Rows[3][4] == DBNull.Value ? 0 : dt.Rows[3][1]);

                Assert.AreEqual(2005, dt.Rows[4][0]);
                Assert.AreEqual(0, dt.Rows[4][1]);
                Assert.AreEqual(1, dt.Rows[4][2]);
                Assert.AreEqual(0, dt.Rows[4][3]);
                Assert.AreEqual(0, dt.Rows[4][4]);

            }
        }


        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        public void Test_PivotOnlyCount(DatabaseType type)
        {
            var tbl = _testTables[type];
            var svr = tbl.Database.Server;

            var lines = new List<CustomLine>();
            
            lines.Add(new CustomLine("SELECT", QueryComponent.SELECT));
            
            lines.Add(new CustomLine("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction });
            lines.Add(new CustomLine("Category as Cat,", QueryComponent.QueryTimeColumn));
            lines.Add(new CustomLine("EventDate as Ev", QueryComponent.QueryTimeColumn){Role = CustomLineRole.Pivot});
            
            lines.Add(new CustomLine("FROM " + tbl.GetFullyQualifiedName(), QueryComponent.FROM));
            
            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("Category,", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate", QueryComponent.GroupBy){Role = CustomLineRole.Pivot});
            
            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines,null,true);

            using (var con = svr.GetConnection())
            {
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
                DataTable dt = new DataTable();
                da.Fill(dt);

                Assert.AreEqual(9,dt.Columns.Count);
                Assert.AreEqual(4,dt.Rows.Count);
                Assert.AreEqual("Cat",dt.Columns[0].ColumnName);


            }
        }

    }


}
