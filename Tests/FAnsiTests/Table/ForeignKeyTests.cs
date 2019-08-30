using System.Collections.Generic;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using NUnit.Framework;
using TypeGuesser;

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
        public void TestForeignKey_OneColumnKey(DatabaseType dbType, bool cascade)
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
            
            DiscoveredRelationship[] relationships = parentTable.DiscoverRelationships();

            Assert.AreEqual(1,relationships.Length);
            
            Assert.AreEqual(parentTable,relationships[0].PrimaryKeyTable);
            Assert.AreEqual(childTable,relationships[0].ForeignKeyTable);
            Assert.AreEqual(1,relationships[0].Keys.Count);

            Assert.AreEqual(parentTable.DiscoverColumns().Single(),relationships[0].Keys.Keys.Single());
            Assert.AreEqual(discovered_fkCol, relationships[0].Keys.Values.Single());

            Assert.AreEqual(parentTable.DiscoverColumns().Single(), discovered_pkCol);

            Assert.AreEqual(relationships[0].Keys[discovered_pkCol],discovered_fkCol);

            Assert.AreEqual(cascade ? CascadeRule.Delete:CascadeRule.NoAction,relationships[0].CascadeDelete);

            var sort1 = new RelationshipTopologicalSort(new[] {childTable, parentTable});
            Assert.AreEqual(sort1.Order[0],parentTable);
            Assert.AreEqual(sort1.Order[1],childTable);

            var sort2 = new RelationshipTopologicalSort(new[] { parentTable,childTable});
            Assert.AreEqual(sort2.Order[0], parentTable);
            Assert.AreEqual(sort2.Order[1], childTable);
            
            childTable.Drop();
            parentTable.Drop();
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestForeignKey_TwoColumnKey(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType, false);

            //cleanup remnants
            foreach (var t in new[] { db.ExpectTable("Child2"), db.ExpectTable("Table2") })
                if (t.Exists())
                    t.Drop();

            foreach (var t in db.DiscoverTables(false))
                t.Drop();

            var parentTable = db.CreateTable("Table2",
                new[]
                {
                    new DatabaseColumnRequest("Id1", "int", false)
                    {
                        IsPrimaryKey = true
                    },
                    new DatabaseColumnRequest("Id2", "int", false)
                    {
                        IsPrimaryKey = true
                    }

                });

            var discovered_pkCol1 = parentTable.DiscoverColumn("Id1");
            var discovered_pkCol2 = parentTable.DiscoverColumn("Id2");

            var requested_fkCol1 = new DatabaseColumnRequest("Parent_Id1", "int");
            var requested_fkCol2 = new DatabaseColumnRequest("Parent_Id2", "int");
            var childTable = db.CreateTable("Child2", new[]
            {
                requested_fkCol1,
                requested_fkCol2,
                
            }, new Dictionary<DatabaseColumnRequest, DiscoveredColumn>()
            {
                {requested_fkCol1,discovered_pkCol1},
                {requested_fkCol2,discovered_pkCol2}
            },true);


            var discovered_fkCol1 = childTable.DiscoverColumn("Parent_Id1");
            var discovered_fkCol2 = childTable.DiscoverColumn("Parent_Id2");

            DiscoveredRelationship[] relationships = parentTable.DiscoverRelationships();

            Assert.AreEqual(1, relationships.Length);

            Assert.AreEqual(parentTable, relationships[0].PrimaryKeyTable);
            Assert.AreEqual(childTable, relationships[0].ForeignKeyTable);

            //should be a composite key of Id1 => Parent_Id1 && Id2 => Parent_Id2
            Assert.AreEqual(2, relationships[0].Keys.Count);

            Assert.AreEqual(discovered_fkCol1, relationships[0].Keys[discovered_pkCol1]);
            Assert.AreEqual(discovered_fkCol2, relationships[0].Keys[discovered_pkCol2]);
            
            childTable.Drop();
            parentTable.Drop();
        }

        [Test]
        public void Test_RelationshipTopologicalSort_UnrelatedTables()
        {
            var db = GetTestDatabase(DatabaseType.MicrosoftSQLServer);

            var cops = db.CreateTable("Cops", new[] {new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string),100))});
            var robbers = db.CreateTable("Robbers", new[] { new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100)) });
            var lawyers = db.CreateTable("Lawyers", new[] { new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100)) });

            var sort = new RelationshipTopologicalSort(new DiscoveredTable[] {cops});
            Assert.AreEqual(cops,sort.Order.Single());
            
            var sort2 = new RelationshipTopologicalSort(new DiscoveredTable[] { cops,robbers,lawyers });
            Assert.AreEqual(cops, sort2.Order[0]);
            Assert.AreEqual(robbers, sort2.Order[1]);
            Assert.AreEqual(lawyers, sort2.Order[2]);

        }
    }
}
