using System.Collections.Generic;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Table;

public class BigIntTests : DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBigInt_Insert(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);
        var tbl = db.CreateTable("MyBigIntTable", [new DatabaseColumnRequest("Col1","bigint",false)]);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        tbl.Insert(new Dictionary<string,object>{ {"Col1",9223372036854775807L} });

        Assert.Multiple(() =>
        {
            Assert.That(tbl.GetRowCount(), Is.EqualTo(1));
            Assert.That(tbl.GetDataTable().Rows[0][0], Is.EqualTo(9223372036854775807L));
        });
        tbl.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBigInt_InsertDataTable(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);
        var tbl = db.CreateTable("MyBigIntTable", [new DatabaseColumnRequest("Col1","bigint",false)]);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using var dt = new DataTable();
        dt.Columns.Add("Col1");
        dt.Rows.Add(9223372036854775807L);

        using(var insert = tbl.BeginBulkInsert())
        {
            insert.Upload(dt);
        }

        Assert.Multiple(() =>
        {
            Assert.That(tbl.GetRowCount(), Is.EqualTo(1));
            Assert.That(tbl.GetDataTable().Rows[0][0], Is.EqualTo(9223372036854775807L));
        });
        tbl.Drop();
    }
}