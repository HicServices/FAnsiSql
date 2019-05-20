using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests.Database
{
    class DatabaseLevelTests : DatabaseTests
    {
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        public void Database_Exists(DatabaseType type)
        {
            var server = GetTestDatabase(type);
            Assert.IsTrue(server.Exists(), "Server " + server + " did not exist");
        }


        [TestCase(DatabaseType.MySql,false)]
        [TestCase(DatabaseType.MicrosoftSQLServer,false)]
        [TestCase(DatabaseType.Oracle,true)]
        public void Test_ExpectDatabase(DatabaseType type, bool upperCase)
        {
            var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
            var server = new DiscoveredServer(helper.GetConnectionStringBuilder("loco","db",null,null));
            var db = server.ExpectDatabase("omg");
            Assert.AreEqual(upperCase?"OMG":"omg",db.GetRuntimeName());
        }
    }
}