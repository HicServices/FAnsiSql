using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table
{
    class TestRename:DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer)] 
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.PostgreSql)]
        public void TestRenamingTable(DatabaseType type)
        {
            var db = GetTestDatabase(type);
            
            var tbl = db.CreateTable("MyTable",new []{new DatabaseColumnRequest("Age",new DatabaseTypeRequest(typeof(int)) )});
            
            Assert.IsTrue(tbl.Exists());
            
            var tbl2 = db.ExpectTable("MYTABLE2");
            Assert.IsFalse(tbl2.Exists());

            tbl.Rename("MYTABLE2");
            
            Assert.IsTrue(tbl.Exists());
            Assert.IsTrue(tbl2.Exists());

            Assert.AreEqual("MYTABLE2",tbl.GetRuntimeName());
            Assert.AreEqual("MYTABLE2",tbl2.GetRuntimeName());

        }
    }
}
