using System;
using System.Collections.Generic;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal class UpdateTests :DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_UpdateTableFromJoin(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Name");
            dt1.Columns.Add("HighScore");

            dt1.Rows.Add("Dave", 100);
            dt1.Rows.Add("Frank", DBNull.Value);
            dt1.Rows.Add("Levo", DBNull.Value);

            tbl1 = db.CreateTable("HighScoresTable", dt1);
        }

        using(var dt2 = new DataTable())
        {
            dt2.Columns.Add("Name");
            dt2.Columns.Add("Score");
            dt2.Rows.Add("Dave", 50);
            dt2.Rows.Add("Frank", 900);
                
            tbl2 = db.CreateTable("NewScoresTable", dt2);
        }

        var syntaxHelper = db.Server.GetQuerySyntaxHelper();

        var updateHelper = syntaxHelper.UpdateHelper;
            
        var queryLines = new List<CustomLine>();

        var highScore = syntaxHelper.EnsureWrapped("HighScore");
        var score = syntaxHelper.EnsureWrapped("Score");
        var name = syntaxHelper.EnsureWrapped("Name");

        queryLines.Add(new CustomLine($"t1.{highScore} = t2.{score}",QueryComponent.SET));
        queryLines.Add(new CustomLine($"t1.{highScore} < t2.{score} OR t1.{highScore} is null",QueryComponent.WHERE));
        queryLines.Add(new CustomLine($"t1.{name} = t2.{name}",QueryComponent.JoinInfoJoin));

        var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

        TestContext.WriteLine($"UPDATE Sql:{sql}");

        using var con = db.Server.GetConnection();
        con.Open();

        var cmd = db.Server.GetCommand(sql, con);
        var affectedRows = cmd.ExecuteNonQuery();

        Assert.AreEqual(1,affectedRows);

        //Frank should have got a new high score of 900
        cmd = db.Server.GetCommand($"SELECT {highScore} from {tbl1.GetFullyQualifiedName()} WHERE {name} = 'Frank'", con);
        Assert.AreEqual(900,cmd.ExecuteScalar());

        //Dave should have his old score of 100
        cmd = db.Server.GetCommand($"SELECT {highScore} from {tbl1.GetFullyQualifiedName()} WHERE {name} = 'Dave'", con);
        Assert.AreEqual(100, cmd.ExecuteScalar());
    }

}