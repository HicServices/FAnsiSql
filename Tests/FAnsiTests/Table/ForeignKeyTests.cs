using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FansiTests;
using NUnit.Framework;

namespace FAnsiTests.Table
{
    class ForeignKeyTests:DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer,true)]
        [TestCase(DatabaseType.MicrosoftSQLServer, false)]
        [TestCase(DatabaseType.MySql, true)]
        [TestCase(DatabaseType.MySql,false)]
        [TestCase(DatabaseType.Oracle, true)]
        [TestCase(DatabaseType.Oracle, false)]
        public void TestMicrosoftForeignKey(DatabaseType dbType, bool cascade)
        {
            var db = GetTestDatabase(dbType,false);

            //cleanup remnants
            foreach (var t in new[] { db.ExpectTable("Child1"), db.ExpectTable("Table1")})
                if(t.Exists())
                    t.Drop();
            
            foreach (var t in db.DiscoverTables(false))
                t.Drop();

            var parentTable = db.CreateTable("Table1",
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

            var childTable = db.CreateTable("Child1", new[]
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
