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

        Assert.Multiple(() =>
        {
            Assert.That(col.AllowNulls);
            Assert.That(col.IsPrimaryKey, Is.False);
        });

        tbl.CreatePrimaryKey(col);
        tbl.CreateIndex("my_index",new List<DiscoveredColumn>() {col}.ToArray());
        col = tbl.DiscoverColumn("A");

        Assert.Multiple(() =>
        {
            Assert.That(col.AllowNulls, Is.False);
            Assert.That(col.IsPrimaryKey);
        });
    }

    //[TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    //public void TestBasicCase_FailHalfWay_SchemaUnchanged(DatabaseType databaseType)
    //{
    //    DiscoveredTable tbl;
    //    // Force column C to be a string otherwise Oracle gets upset by TypeGuesser mis-guessing the nulls as boolean
    //    var c = new DataColumn("C", typeof(string));
    //    c.SetDoNotReType(true);
    //    using (var dt = new DataTable("Fish"))
    //    {
    //        dt.Columns.Add("A");
    //        dt.Columns.Add("B");
    //        dt.Columns.Add(c);

    //        dt.Rows.Add("a1", "b1", null);
    //        dt.Rows.Add("a2", null, null);
    //        dt.Rows.Add("a3", "b2", null);

    //        var db = GetTestDatabase(databaseType);

    //        tbl = db.CreateTable("Fish", dt);
    //    }

    //    var colA = tbl.DiscoverColumn("A");
    //    var colB = tbl.DiscoverColumn("B");

    //    Assert.Multiple(() =>
    //    {
    //        //Pre state
    //        Assert.That(colA.AllowNulls);
    //        Assert.That(colA.IsPrimaryKey, Is.False);
    //        Assert.That(colB.AllowNulls);
    //        Assert.That(colB.IsPrimaryKey, Is.False);
    //    });

    //    var ex = Assert.Throws<AlterFailedException>(()=>tbl.CreatePrimaryKey(colA, colB));
    //    Assert.Multiple(() =>
    //    {
    //        Assert.That(ex?.Message.Contains("Failed to create primary key on table") ?? false);
    //        Assert.That(ex?.InnerException, Is.Not.InstanceOf(typeof(AggregateException)));
    //        Assert.That(ex?.InnerException, Is.InstanceOf<DbException>());
    //    });

    //    colA = tbl.DiscoverColumn("A");
    //    colB = tbl.DiscoverColumn("B");

    //    Assert.Multiple(() =>
    //    {
    //        //Post state should exactly match
    //        Assert.That(colA.IsPrimaryKey, Is.False);
    //        Assert.That(colB.IsPrimaryKey, Is.False);
    //    });
    //}
}