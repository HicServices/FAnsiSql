using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;

namespace FAnsiTests.Table
{
    class UpdateTests :DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MySql)]
        public void Test_UpdateTableFromJoin(DatabaseType dbType)
        {
            var dt1 = new DataTable();
            dt1.Columns.Add("Name");
            dt1.Columns.Add("HighScore");

            dt1.Rows.Add("Dave", 100);
            dt1.Rows.Add("Frank", DBNull.Value);
            dt1.Rows.Add("Levo", DBNull.Value);

            var dt2 = new DataTable();
            dt2.Columns.Add("Name");
            dt2.Columns.Add("Score");
            dt2.Rows.Add("Dave", 50);
            dt2.Rows.Add("Frank", 900);

            DiscoveredDatabase db = GetTestDatabase(dbType);

            var tbl1 = db.CreateTable("HighScoresTable", dt1);
            var tbl2 = db.CreateTable("NewScoresTable", dt2);

            var updateHelper = db.Server.GetQuerySyntaxHelper().UpdateHelper;
            
            List<CustomLine> queryLines = new List<CustomLine>();
            
            queryLines.Add(new CustomLine("t1.HighScore = t2.Score",QueryComponent.SET));
            queryLines.Add(new CustomLine("t1.HighScore < t2.Score OR t1.HighScore is null",QueryComponent.WHERE));
            queryLines.Add(new CustomLine("t1.Name = t2.Name",QueryComponent.JoinInfoJoin));

            string sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            Console.WriteLine("UPDATE Sql:" + sql);

            using (var con = db.Server.GetConnection())
            {
                con.Open();

                DbCommand cmd = db.Server.GetCommand(sql, con);
                int affectedRows = cmd.ExecuteNonQuery();

                Assert.AreEqual(1,affectedRows);

                //Frank should have got a new high score of 900
                cmd = db.Server.GetCommand(string.Format("SELECT HighScore from {0} WHERE Name = 'Frank'",tbl1.GetFullyQualifiedName()), con);
                Assert.AreEqual(900,cmd.ExecuteScalar());

                //Dave should have his old score of 100
                cmd = db.Server.GetCommand(string.Format("SELECT HighScore from {0} WHERE Name = 'Dave'", tbl1.GetFullyQualifiedName()), con);
                Assert.AreEqual(100, cmd.ExecuteScalar());
            }
        }

    }
}
