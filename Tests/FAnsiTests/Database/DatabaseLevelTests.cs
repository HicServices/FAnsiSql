using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Database
{
    class DatabaseLevelTests : DatabaseTests
    {
        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void Database_Exists(DatabaseType type)
        {
            var server = GetTestDatabase(type);
            Assert.IsTrue(server.Exists(), "Server " + server + " did not exist");
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
            Assert.AreEqual(upperCase?"OMG":"omg",db.GetRuntimeName());
        }

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void Test_CreateSchema(DatabaseType type)
        {
            var db = GetTestDatabase(type);

            Assert.DoesNotThrow(()=>db.CreateSchema("Fr ank"));
            Assert.DoesNotThrow(()=>db.CreateSchema("Fr ank"));

            db.Server.GetQuerySyntaxHelper().EnsureWrapped("Fr ank");

            if (type == DatabaseType.MicrosoftSQLServer || type == DatabaseType.PostgreSql)
            {
                var tbl = db.CreateTable("Heyyy",
                    new[] {new DatabaseColumnRequest("fff", new DatabaseTypeRequest(typeof(string), 10))},"Fr ank");

                Assert.IsTrue(tbl.Exists());

                if(type == DatabaseType.MicrosoftSQLServer)
                    Assert.AreEqual("Fr ank",tbl.Schema);
            }
        }
    }
}