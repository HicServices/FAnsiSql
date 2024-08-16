using System.Collections.Generic;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

internal sealed class ForeignKeyTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void TestForeignKey_OneColumnKey(DatabaseType dbType, bool cascade)
    {
        var db = GetTestDatabase(dbType);


        var parentTable = db.CreateTable("Table1",
            [
                new DatabaseColumnRequest("Id", "int", false)
                {
                    IsAutoIncrement = true,
                    IsPrimaryKey = true
                }
            ]);

        var discovered_pkCol = parentTable.DiscoverColumn("Id");
        var requested_fkCol = new DatabaseColumnRequest("Parent_Id", "int");

        var childTable = db.CreateTable("Child1",
        [
            requested_fkCol,
            new DatabaseColumnRequest("SomeNumber", "int")
        ], new Dictionary<DatabaseColumnRequest, DiscoveredColumn>
        {
            {requested_fkCol,discovered_pkCol}

        }, cascade);

        var discovered_fkCol = childTable.DiscoverColumn("Parent_Id");

        var relationships = parentTable.DiscoverRelationships();

        Assert.That(relationships, Has.Length.EqualTo(1));

        Assert.Multiple(() =>
        {
            Assert.That(relationships[0].PrimaryKeyTable, Is.EqualTo(parentTable));
            Assert.That(relationships[0].ForeignKeyTable, Is.EqualTo(childTable));
            Assert.That(relationships[0].Keys, Has.Count.EqualTo(1));
        });

        Assert.Multiple(() =>
        {
            Assert.That(relationships[0].Keys.Keys.Single(), Is.EqualTo(parentTable.DiscoverColumns().Single()));
            Assert.That(relationships[0].Keys.Values.Single(), Is.EqualTo(discovered_fkCol));

            Assert.That(discovered_pkCol, Is.EqualTo(parentTable.DiscoverColumns().Single()));

            Assert.That(discovered_fkCol, Is.EqualTo(relationships[0].Keys[discovered_pkCol]));

            Assert.That(relationships[0].CascadeDelete, Is.EqualTo(cascade ? CascadeRule.Delete : CascadeRule.NoAction));
        });

        var sort1 = new RelationshipTopologicalSort([childTable, parentTable]);
        Assert.Multiple(() =>
        {
            Assert.That(parentTable, Is.EqualTo(sort1.Order[0]));
            Assert.That(childTable, Is.EqualTo(sort1.Order[1]));
        });

        var sort2 = new RelationshipTopologicalSort([parentTable, childTable]);
        Assert.Multiple(() =>
        {
            Assert.That(parentTable, Is.EqualTo(sort2.Order[0]));
            Assert.That(childTable, Is.EqualTo(sort2.Order[1]));
        });

        childTable.Drop();
        parentTable.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestForeignKey_TwoColumnKey(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var parentTable = db.CreateTable("Table2",
            [
                new DatabaseColumnRequest("Id1", "int", false)
                {
                    IsPrimaryKey = true
                },
                new DatabaseColumnRequest("Id2", "int", false)
                {
                    IsPrimaryKey = true
                }

            ]);

        var discovered_pkCol1 = parentTable.DiscoverColumn("Id1");
        var discovered_pkCol2 = parentTable.DiscoverColumn("Id2");

        var requested_fkCol1 = new DatabaseColumnRequest("Parent_Id1", "int");
        var requested_fkCol2 = new DatabaseColumnRequest("Parent_Id2", "int");
        var childTable = db.CreateTable("Child2",
        [
            requested_fkCol1,
            requested_fkCol2

        ], new Dictionary<DatabaseColumnRequest, DiscoveredColumn>
        {
            {requested_fkCol1,discovered_pkCol1},
            {requested_fkCol2,discovered_pkCol2}
        },true);


        var discovered_fkCol1 = childTable.DiscoverColumn("Parent_Id1");
        var discovered_fkCol2 = childTable.DiscoverColumn("Parent_Id2");

        var relationships = parentTable.DiscoverRelationships();

        Assert.That(relationships, Has.Length.EqualTo(1));

        Assert.Multiple(() =>
        {
            Assert.That(relationships[0].PrimaryKeyTable, Is.EqualTo(parentTable));
            Assert.That(relationships[0].ForeignKeyTable, Is.EqualTo(childTable));

            //should be a composite key of Id1 => Parent_Id1 && Id2 => Parent_Id2
            Assert.That(relationships[0].Keys, Has.Count.EqualTo(2));
        });

        Assert.Multiple(() =>
        {
            Assert.That(relationships[0].Keys[discovered_pkCol1], Is.EqualTo(discovered_fkCol1));
            Assert.That(relationships[0].Keys[discovered_pkCol2], Is.EqualTo(discovered_fkCol2));
        });

        childTable.Drop();
        parentTable.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void Test_ThreeTables_OnePrimary(DatabaseType dbType, bool useTransaction)
    {
        /*       t2
         *     ↙
         *  t1
         *    ↖
         *      t3
         */

        var db = GetTestDatabase(dbType);

        var t2 = db.CreateTable("T2",
        [
            new DatabaseColumnRequest("c2", new DatabaseTypeRequest(typeof(int)))
        ]);

        var t3 = db.CreateTable("T3",
        [
            new DatabaseColumnRequest("c3", new DatabaseTypeRequest(typeof(int)))
        ]);

        var t1 = db.CreateTable("T1",
        [
            new DatabaseColumnRequest("c1", new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true}
        ]);

        var c1 = t1.DiscoverColumns().Single();
        var c2 = t2.DiscoverColumns().Single();
        var c3 = t3.DiscoverColumns().Single();

        DiscoveredRelationship constraint1;
        DiscoveredRelationship constraint2;

        if (useTransaction)
        {
            using var con = t1.Database.Server.BeginNewTransactedConnection();
            constraint1 = t1.AddForeignKey(c2,c1,true,null,new DatabaseOperationArgs {TransactionIfAny = con.ManagedTransaction});
            constraint2 = t1.AddForeignKey(c3,c1,true,"FK_Lol",new DatabaseOperationArgs {TransactionIfAny = con.ManagedTransaction});
            con.ManagedTransaction?.CommitAndCloseConnection();
        }
        else
        {
            constraint1 = t1.AddForeignKey(c2,c1,true);
            constraint2 = t1.AddForeignKey(c3,c1,true,"FK_Lol");
        }

        Assert.Multiple(() =>
        {
            Assert.That(constraint1, Is.Not.Null);
            Assert.That(constraint2, Is.Not.Null);
            Assert.That(constraint1.Name, Is.EqualTo("FK_T2_T1").IgnoreCase);
            Assert.That(constraint2.Name, Is.EqualTo("FK_Lol").IgnoreCase);
        });

        var sort2 = new RelationshipTopologicalSort([t1,t2,t3]).Order.ToList();


        Assert.That(sort2, Does.Contain(t1));
        Assert.That(sort2, Does.Contain(t2));
        Assert.That(sort2, Does.Contain(t3));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_ThreeTables_TwoPrimary(DatabaseType dbType)
    {
        /*  t1
         *      ↖
         *       t3
         *      ↙
         *  t2
         */

        var db = GetTestDatabase(dbType);

        var t1 = db.CreateTable("T1",
        [
            new DatabaseColumnRequest("c1", new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true}
        ]);

        var t2 = db.CreateTable("T2",
        [
            new DatabaseColumnRequest("c2", new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true}
        ]);

        var t3 = db.CreateTable("T3",
        [
            new DatabaseColumnRequest("c3", new DatabaseTypeRequest(typeof(int)))
        ]);

        var c1 = t1.DiscoverColumns().Single();
        var c2 = t2.DiscoverColumns().Single();
        var c3 = t3.DiscoverColumns().Single();

        var constraint1 = t1.AddForeignKey(c3,c1,true);
        var constraint2 = t1.AddForeignKey(c3,c2,true);

        Assert.Multiple(() =>
        {
            Assert.That(constraint1, Is.Not.Null);
            Assert.That(constraint2, Is.Not.Null);
        });

        var sort2 = new RelationshipTopologicalSort([t1,t2,t3]).Order.ToList();

        Assert.That(sort2, Does.Contain(t1));
        Assert.That(sort2, Does.Contain(t2));
        Assert.That(sort2, Does.Contain(t3));
    }

    [Test]
    public void Test_RelationshipTopologicalSort_UnrelatedTables()
    {
        var db = GetTestDatabase(DatabaseType.MicrosoftSQLServer);

        var cops = db.CreateTable("Cops", [new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string),100))]);
        var robbers = db.CreateTable("Robbers", [new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))]);
        var lawyers = db.CreateTable("Lawyers", [new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))]);

        var sort = new RelationshipTopologicalSort([cops]);
        Assert.That(sort.Order.Single(), Is.EqualTo(cops));

        var sort2 = new RelationshipTopologicalSort([cops,robbers,lawyers]);
        Assert.Multiple(() =>
        {
            Assert.That(sort2.Order[0], Is.EqualTo(cops));
            Assert.That(sort2.Order[1], Is.EqualTo(robbers));
            Assert.That(sort2.Order[2], Is.EqualTo(lawyers));
        });

    }
}