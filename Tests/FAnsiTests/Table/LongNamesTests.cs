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

        var sb = new StringBuilder();

        for (var i = 0; i < db.Server.GetQuerySyntaxHelper().MaximumTableLength; i++)
            sb.Append('a');

        var sb2 = new StringBuilder();
        for (var i = 0; i < db.Server.GetQuerySyntaxHelper().MaximumColumnLength; i++)
            sb2.Append('b');

        //128 characters long
        var tableName = sb.ToString();
        var columnName = sb2.ToString();
            
        var tbl = db.CreateTable(tableName,new DatabaseColumnRequest[]{new(columnName,new DatabaseTypeRequest(typeof(string),100))});

        Assert.IsTrue(tbl.Exists());
        StringAssert.AreEqualIgnoringCase(tableName,tbl.GetRuntimeName());

        var col = tbl.DiscoverColumn(columnName);
        Assert.IsNotNull(col);
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
            
        Assert.IsTrue(db2.Exists());
        StringAssert.AreEqualIgnoringCase(sb.ToString(),db2.GetRuntimeName());
            
        db2.Drop();
    }
}