using System.Text;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

internal class LongNamesTests : DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_LongTableName_CreateAndReadBack(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var tableName = new StringBuilder(db.Server.GetQuerySyntaxHelper().MaximumTableLength).Append('a', db.Server.GetQuerySyntaxHelper().MaximumTableLength).ToString();
        var columnName = new StringBuilder(db.Server.GetQuerySyntaxHelper().MaximumColumnLength).Append('b', db.Server.GetQuerySyntaxHelper().MaximumColumnLength).ToString();

        var tbl = db.CreateTable(tableName,new DatabaseColumnRequest[]{new(columnName,new DatabaseTypeRequest(typeof(string),100))});

        Assert.That(tbl.Exists());
        StringAssert.AreEqualIgnoringCase(tableName,tbl.GetRuntimeName());

        var col = tbl.DiscoverColumn(columnName);
        Assert.That(col, Is.Not.Null);
        StringAssert.AreEqualIgnoringCase(columnName,col.GetRuntimeName());
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_LongDatabaseNames_CreateAndReadBack(DatabaseType dbType)
    {
        AssertCanCreateDatabases();

        var db = GetTestDatabase(dbType);

        var sb = new StringBuilder();

        for (var i = 0; i < db.Server.GetQuerySyntaxHelper().MaximumDatabaseLength; i++)
            sb.Append('a');

        var db2 = db.Server.ExpectDatabase(sb.ToString());
        db2.Create(true);

        Assert.That(db2.Exists());
        StringAssert.AreEqualIgnoringCase(sb.ToString(),db2.GetRuntimeName());

        db2.Drop();
    }
}