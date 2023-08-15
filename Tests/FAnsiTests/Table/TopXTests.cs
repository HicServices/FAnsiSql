﻿using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System;
using System.Data;
using FAnsi.Discovery;

namespace FAnsiTests.Table;

internal class TopXTests :DatabaseTests
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
            Assert.AreEqual(asc?1:4,db.Server.GetCommand(sql,con).ExecuteScalar());
        }

        var dtTopX = tbl.GetDataTable(1);
        Assert.AreEqual(1,dtTopX.Rows.Count);
        Assert.AreEqual(1,dtTopX.Rows[0]["F"]);


        using(var con = db.Server.GetConnection())
        {
            con.Open();
            var sqlcol = tbl.DiscoverColumn("X").GetTopXSql(1,false);

            Assert.AreEqual(DBNull.Value,db.Server.GetCommand(sqlcol,con).ExecuteScalar());

            sqlcol = tbl.DiscoverColumn("X").GetTopXSql(1,true);

            Assert.AreEqual("fish",db.Server.GetCommand(sqlcol,con).ExecuteScalar());
        }
    }
}