using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Database;

internal class DatabaseLevelTests : DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Database_Exists(DatabaseType type)
    {
        var server = GetTestDatabase(type);
        Assert.That(server.Exists(), "Server " + server + " did not exist");
    }


    [TestCase(DatabaseType.MySql,false)]
    [TestCase(DatabaseType.MicrosoftSQLServer,false)]
    [TestCase(DatabaseType.Oracle,true)]
    [TestCase(DatabaseType.PostgreSql,false)]
    public void Test_ExpectDatabase(DatabaseType type, bool upperCase)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var server = new DiscoveredServer(helper.GetConnectionStringBuilder("loco","db","frank","kangaro"));
        var db = server.ExpectDatabase("omg");
        Assert.That(db.GetRuntimeName(), Is.EqualTo(upperCase ?"OMG":"omg"));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_CreateSchema(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        Assert.DoesNotThrow(()=>db.CreateSchema("Fr ank"));
        Assert.DoesNotThrow(()=>db.CreateSchema("Fr ank"));

        db.Server.GetQuerySyntaxHelper().EnsureWrapped("Fr ank");

        if (type is DatabaseType.MicrosoftSQLServer or DatabaseType.PostgreSql)
        {
            var tbl = db.CreateTable("Heyyy",
                [new DatabaseColumnRequest("fff", new DatabaseTypeRequest(typeof(string), 10))],"Fr ank");

            Assert.That(tbl.Exists());

            if(type == DatabaseType.MicrosoftSQLServer)
                Assert.That(tbl.Schema, Is.EqualTo("Fr ank"));
        }
    }
}