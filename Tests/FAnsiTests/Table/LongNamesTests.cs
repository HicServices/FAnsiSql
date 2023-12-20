using System.Text;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using NUnit.Framework.Legacy;
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

        var tbl = db.CreateTable(tableName,[new(columnName,new DatabaseTypeRequest(typeof(string),100))]);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists());
            Assert.That(tbl.GetRuntimeName(), Is.EqualTo(tableName).IgnoreCase);
        });

        var col = tbl.DiscoverColumn(columnName);
        Assert.That(col, Is.Not.Null);
        Assert.That(col.GetRuntimeName(), Is.EqualTo(columnName).IgnoreCase);
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

        Assert.Multiple(() =>
        {
            Assert.That(db2.Exists());
            Assert.That(db2.GetRuntimeName(), Is.EqualTo(sb.ToString()).IgnoreCase);
        });

        db2.Drop();
    }
}