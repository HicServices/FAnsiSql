using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace FAnsiTests.Aggregation
{
    class PivotAggregationTests:AggregationTests
    {
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
            lines.Add(new CustomLine("EventDate as Ev", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot });

            lines.Add(new CustomLine("FROM " + tbl.GetFullyQualifiedName(), QueryComponent.FROM));

            lines.Add(new CustomLine("GROUP BY", QueryComponent.GroupBy));
            lines.Add(new CustomLine("Category,", QueryComponent.GroupBy));
            lines.Add(new CustomLine("EventDate", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot });

            var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null, true);

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

                Assert.AreEqual(9, dt.Columns.Count);
                Assert.AreEqual(4, dt.Rows.Count);
                Assert.AreEqual("Cat", dt.Columns[0].ColumnName);
            }
        }
    }
}
