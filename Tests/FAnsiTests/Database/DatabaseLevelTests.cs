using FAnsi;
using NUnit.Framework;

namespace FAnsiSqlTests.Database
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
    }
}