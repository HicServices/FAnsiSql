using System.Collections.Generic;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

class ForeignKeyTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void TestForeignKey_OneColumnKey(DatabaseType dbType, bool cascade)
    {
        var db = GetTestDatabase(dbType);


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

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestForeignKey_TwoColumnKey(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

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

        var t2 = db.CreateTable("T2", new DatabaseColumnRequest[]
        {
            new DatabaseColumnRequest("c2", new DatabaseTypeRequest(typeof(int)))
        });

        var t3 = db.CreateTable("T3", new DatabaseColumnRequest[]
        {
            new DatabaseColumnRequest("c3", new DatabaseTypeRequest(typeof(int)))
        });
            
        var t1 = db.CreateTable("T1", new DatabaseColumnRequest[]
        {
            new DatabaseColumnRequest("c1", new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true}
        });

        var c1 = t1.DiscoverColumns().Single();
        var c2 = t2.DiscoverColumns().Single();
        var c3 = t3.DiscoverColumns().Single();

        DiscoveredRelationship constraint1;
        DiscoveredRelationship constraint2;

        if (useTransaction)
        {
            using (var con = t1.Database.Server.BeginNewTransactedConnection())
            {
                constraint1 = t1.AddForeignKey(c2,c1,true,null,new DatabaseOperationArgs(){TransactionIfAny = con.ManagedTransaction});
                constraint2 = t1.AddForeignKey(c3,c1,true,"FK_Lol",new DatabaseOperationArgs(){TransactionIfAny = con.ManagedTransaction});
                con.ManagedTransaction.CommitAndCloseConnection();
            }
        }
        else
        {
            constraint1 = t1.AddForeignKey(c2,c1,true);
            constraint2 = t1.AddForeignKey(c3,c1,true,"FK_Lol");
        }

            
            

        Assert.IsNotNull(constraint1);
        Assert.IsNotNull(constraint2);

        StringAssert.AreEqualIgnoringCase("FK_T2_T1",constraint1.Name);
        StringAssert.AreEqualIgnoringCase("FK_Lol",constraint2.Name);

        var sort2 = new RelationshipTopologicalSort(new DiscoveredTable[] { t1,t2,t3 });
            
            
        Assert.Contains(t1, sort2.Order.ToList());
        Assert.Contains(t2, sort2.Order.ToList());
        Assert.Contains(t3, sort2.Order.ToList());
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
            
        var t1 = db.CreateTable("T1", new DatabaseColumnRequest[]
        {
            new DatabaseColumnRequest("c1", new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true}
        });

        var t2 = db.CreateTable("T2", new DatabaseColumnRequest[]
        {
            new DatabaseColumnRequest("c2", new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true}
        });

        var t3 = db.CreateTable("T3", new DatabaseColumnRequest[]
        {
            new DatabaseColumnRequest("c3", new DatabaseTypeRequest(typeof(int)))
        });

        var c1 = t1.DiscoverColumns().Single();
        var c2 = t2.DiscoverColumns().Single();
        var c3 = t3.DiscoverColumns().Single();

        var constraint1 = t1.AddForeignKey(c3,c1,true);
        var constraint2 = t1.AddForeignKey(c3,c2,true);

        Assert.IsNotNull(constraint1);
        Assert.IsNotNull(constraint2);

        var sort2 = new RelationshipTopologicalSort(new DiscoveredTable[] { t1,t2,t3 });
            
        Assert.Contains(t1, sort2.Order.ToList());
        Assert.Contains(t2, sort2.Order.ToList());
        Assert.Contains(t3, sort2.Order.ToList());
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