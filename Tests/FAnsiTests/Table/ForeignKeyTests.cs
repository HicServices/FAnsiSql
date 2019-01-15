using System.Collections.Generic;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FansiTests;
using NUnit.Framework;

namespace FAnsiTests.Table
{
    class ForeignKeyTests:DatabaseTests
    {
        [TestCase(true)]
        [TestCase(false)]
        public void TestMicrosoftForeignKey(bool cascade)
        {
            var db = GetTestDatabase(DatabaseType.MicrosoftSQLServer);

            var parentTable = db.CreateTable("Table",
                new[]
                {
                    new DatabaseColumnRequest("Id", "int", false)
                    {
                        IsAutoIncrement = true,
                        IsPrimaryKey = true
                    }
                });

            var discovered_pkCol = parentTable.DiscoverColumn("Id");
            var requested_fkCol = new DatabaseColumnRequest("Parent_Id", "int");

            var childTable = db.CreateTable("Child", new[]
            {
                requested_fkCol,
                new DatabaseColumnRequest("SomeNumber", "int")
            }, new Dictionary<DatabaseColumnRequest, DiscoveredColumn>()
            {
                {requested_fkCol,discovered_pkCol}
            
            }, cascade);

            var discovered_fkCol = childTable.DiscoverColumn("Parent_Id");
            
            DiscoveredRelationship[] relationships = parentTable.DiscoveredRelationships();

            Assert.AreEqual(1,relationships.Length);
            
            Assert.AreEqual(parentTable,relationships[0].PrimaryKeyTable);
            Assert.AreEqual(childTable,relationships[0].ForeignKeyTable);
            Assert.AreEqual(1,relationships[0].Keys.Count);

            Assert.AreEqual(parentTable.DiscoverColumns().Single(),relationships[0].Keys.Keys.Single());
            Assert.AreEqual(discovered_fkCol, relationships[0].Keys.Values.Single());

            Assert.AreEqual(parentTable.DiscoverColumns().Single(), discovered_pkCol);

            Assert.AreEqual(relationships[0].Keys[discovered_pkCol],discovered_fkCol);

            Assert.AreEqual(cascade ? CascadeRule.Delete:CascadeRule.NoAction,relationships[0].CascadeDelete);

            childTable.Drop();
            parentTable.Drop();
        }
    }
}
