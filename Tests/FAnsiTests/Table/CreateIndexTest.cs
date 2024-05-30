using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Exceptions;
using FAnsi.Extensions;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class CreateIndexTest: DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBasicCase_IndexCreated(DatabaseType databaseType)
    {
        // Force columns B and C to be strings otherwise Oracle gets upset by TypeGuesser mis-guessing the nulls as boolean
        var b=new DataColumn("B", typeof(string));
        b.SetDoNotReType(true);
        var c=new DataColumn("C", typeof(string));
        c.SetDoNotReType(true);
        DiscoveredTable tbl;
        using (var dt = new DataTable("Fish"))
        {
            dt.Columns.Add("A");
            dt.Columns.Add(b);
            dt.Columns.Add(c);

            dt.Rows.Add("a1", null, null);
            dt.Rows.Add("a2", null, null);
            dt.Rows.Add("a3", null, null);

            var db = GetTestDatabase(databaseType);

            tbl = db.CreateTable("Fish", dt);
        }

        var col = tbl.DiscoverColumn("A");

        Assert.DoesNotThrow(() => tbl.CreateIndex("my_index", [col]));
        Assert.DoesNotThrow(() => tbl.DropIndex("my_index"));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void TestBasicCase_DeleteIndexThatDoesntExist(DatabaseType databaseType)
    {
        // Force columns B and C to be strings otherwise Oracle gets upset by TypeGuesser mis-guessing the nulls as boolean
        var b = new DataColumn("B", typeof(string));
        b.SetDoNotReType(true);
        var c = new DataColumn("C", typeof(string));
        c.SetDoNotReType(true);
        DiscoveredTable tbl;
        using (var dt = new DataTable("Fish"))
        {
            dt.Columns.Add("A");
            dt.Columns.Add(b);
            dt.Columns.Add(c);

            dt.Rows.Add("a1", null, null);
            dt.Rows.Add("a2", null, null);
            dt.Rows.Add("a3", null, null);

            var db = GetTestDatabase(databaseType);

            tbl = db.CreateTable("Fish", dt);
        }

        var col = tbl.DiscoverColumn("A");

        Assert.Throws<AlterFailedException>(() => tbl.DropIndex("my_index"));
    }
}