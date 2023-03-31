using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using NUnit.Framework;

namespace FAnsiTests.Equality;

internal class EqualityTests_ServerAndDatabase
{
    [OneTimeSetUp]
    public void SetUp()
    {
        ImplementationManager.Load(
            typeof(MicrosoftSQLImplementation).Assembly,
            typeof(OracleImplementation).Assembly,
            typeof(MySqlImplementation).Assembly
        );
    }

    [TestCase("Server=fish fish fish", DatabaseType.MicrosoftSQLServer,"Server=fish fish fish", DatabaseType.MicrosoftSQLServer)]
    [TestCase(null, DatabaseType.MicrosoftSQLServer, null, DatabaseType.MicrosoftSQLServer)]
    [TestCase(null, DatabaseType.MicrosoftSQLServer, "", DatabaseType.MicrosoftSQLServer)]
    public void EqualityTest_DiscoveredServer_AreEqual(string constr1, DatabaseType type1,string constr2, DatabaseType type2)
    {
        ImplementationManager.Load(
            typeof(MicrosoftSQLImplementation).Assembly,
            typeof(OracleImplementation).Assembly,
            typeof(MySqlImplementation).Assembly
        );

        var s1 = new DiscoveredServer(constr1, type1);
        var s2 = new DiscoveredServer(constr2, type2);
            
        Assert.AreEqual(s1,s2);
        Assert.AreEqual(s1.GetHashCode(),s2.GetHashCode());

        Assert.AreEqual(s1.ExpectDatabase("MyDb"), s2.ExpectDatabase("MyDb"));
        Assert.AreEqual(s1.ExpectDatabase("MyDb").GetHashCode(), s2.ExpectDatabase("MyDb").GetHashCode());

        Assert.AreEqual(s1.ExpectDatabase("Mydb"), s2.ExpectDatabase("MyDb"));
        Assert.AreEqual(s1.ExpectDatabase("Mydb").GetHashCode(), s2.ExpectDatabase("MyDb").GetHashCode());
            
        Assert.AreNotEqual(s1.ExpectDatabase("MyDb"), s2.ExpectDatabase("MyDb2"));

        //This does not affect things since we are expecting a specific database anyway
        s1.ChangeDatabase("Dave");
        Assert.AreNotEqual(s1,s2);
        Assert.AreEqual(s1.ExpectDatabase("MyDb"), s2.ExpectDatabase("MyDb"));
        Assert.AreEqual(s1.ExpectDatabase("MyDb").GetHashCode(), s2.ExpectDatabase("MyDb").GetHashCode());

    }

    [TestCase("Server=fish fish fish", DatabaseType.MicrosoftSQLServer, "Server=fish fish fish;Integrated Security=true", DatabaseType.MicrosoftSQLServer)]
    [TestCase("Server=fish fish fish", DatabaseType.MicrosoftSQLServer, "Server=fish fish fish", DatabaseType.MySql)]
    [TestCase(null, DatabaseType.MicrosoftSQLServer, "", DatabaseType.MySql)]
    public void EqualityTest_DiscoveredServer_AreNotEqual(string constr1, DatabaseType type1, string constr2, DatabaseType type2)
    {
        var s1 = new DiscoveredServer(constr1, type1);
        var s2 = new DiscoveredServer(constr2, type2);
            
        Assert.AreNotEqual(s1,s2);
        Assert.AreNotEqual(s1.ExpectDatabase("MyDb"), s2.ExpectDatabase("MyDb"));
    }
}