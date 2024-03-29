﻿using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Equality;

internal sealed class EqualityTests_TableAndColumn
{
    [OneTimeSetUp]
    public void SetUp()
    {
    }

    [TestCase("MyTable",null,"MyTable",null)]
    [TestCase("MyTable", null, "myTable", null)]
    [TestCase("MyTable", null, "MyTable", "dbo")]
    [TestCase("MyTable", null, "MyTable", "dBo")]
    [TestCase("MyTable", null, "MyTable", "")]
    [TestCase("MyTable", "", "MyTable", "dbo")]
    public void EqualityTest_DiscoveredTable_AreEqual(string table1, string? schema1, string table2, string? schema2)
    {
        var s = new DiscoveredServer("Server=fish", DatabaseType.MicrosoftSQLServer);

        var db = s.ExpectDatabase("MyDb");
        var db2 = s.ExpectDatabase("MyDb");

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(db, db2), Is.False);
            Assert.That(db, Is.EqualTo(db2));
        });

        var t1 = db.ExpectTable(table1,schema1);
        var t2 = db2.ExpectTable(table2,schema2);

        Assert.That(t2, Is.EqualTo(t1));
        Assert.That(t2.GetHashCode(), Is.EqualTo(t1.GetHashCode()));
    }


    [TestCase(DatabaseType.MicrosoftSQLServer, "Server=fish", "MyDb", "MyTable", null, DatabaseType.MicrosoftSQLServer, "Server=fish", "MyDb", "MyTable2", null)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "Server=fish", "MyDb", "MyTable", null, DatabaseType.MicrosoftSQLServer, "Server=fish", "MyDb2", "MyTable", null)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "Server=fish", "MyDb", "MyTable", null, DatabaseType.MicrosoftSQLServer, "Server=fish2", "MyDb", "MyTable", null)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "Server=fish", "MyDb", "MyTable", null, DatabaseType.MySql, "Server=fish", "MyDb", "MyTable", null)]
    public void EqualityTest_DiscoveredTable_AreNotEqual(DatabaseType type1, string constr1, string dbname1, string tablename1, string? schema1, DatabaseType type2, string constr2, string dbname2, string tablename2, string? schema2)
    {
        var s1 = new DiscoveredServer(constr1, type1);
        var s2 = new DiscoveredServer(constr2, type2);

        var db1 = s1.ExpectDatabase(dbname1);
        var db2 = s2.ExpectDatabase(dbname2);

        var t1 = db1.ExpectTable(tablename1, schema1);
        var t2 = db2.ExpectTable(tablename2, schema2);

        Assert.That(t2, Is.Not.EqualTo(t1));
    }
}