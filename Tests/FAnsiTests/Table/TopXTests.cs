using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System;
using System.Data;
using FAnsi.Discovery;

namespace FAnsiTests.Table;

internal sealed class TopXTests :DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void Test_TopX_OrderBy(DatabaseType type,bool asc)
    {
        var db = GetTestDatabase(type);

        DiscoveredTable tbl;
        using (var dt = new DataTable())
        {
            dt.Columns.Add("F");
            dt.Columns.Add("X");

            dt.Rows.Add(1,DBNull.Value);
            dt.Rows.Add(2,"fish");
            dt.Rows.Add(3,"fish");
            dt.Rows.Add(4,"fish");

            tbl = db.CreateTable("MyTopXTable",dt);
        }

        var topx = tbl.GetQuerySyntaxHelper().HowDoWeAchieveTopX(1);

        var f = tbl.GetQuerySyntaxHelper().EnsureWrapped("F");

        var sql = topx.Location switch
        {
            QueryComponent.SELECT =>
                $"SELECT {topx.SQL} {f} FROM {tbl.GetFullyQualifiedName()} ORDER BY {f} {(asc ? "ASC" : "DESC")}",
            QueryComponent.Postfix =>
                $"SELECT {f} FROM {tbl.GetFullyQualifiedName()} ORDER BY {f} {(asc ? "ASC " : "DESC ")}{topx.SQL}",
            _ => throw new ArgumentOutOfRangeException(nameof(type),$"Did not expect location {topx.Location} for {type}")
        };

        using(var con = db.Server.GetConnection())
        {
            con.Open();
            Assert.That(db.Server.GetCommand(sql,con).ExecuteScalar(), Is.EqualTo(asc ?1:4));
        }

        var dtTopX = tbl.GetDataTable(1);
        Assert.That(dtTopX.Rows, Has.Count.EqualTo(1));
        Assert.That(dtTopX.Rows[0]["F"], Is.EqualTo(1));


        using(var con = db.Server.GetConnection())
        {
            con.Open();
            var sqlcol = tbl.DiscoverColumn("X").GetTopXSql(1,false);

            Assert.That(db.Server.GetCommand(sqlcol,con).ExecuteScalar(), Is.EqualTo(DBNull.Value));

            sqlcol = tbl.DiscoverColumn("X").GetTopXSql(1,true);

            Assert.That(db.Server.GetCommand(sqlcol,con).ExecuteScalar(), Is.EqualTo("fish"));
        }
    }
}