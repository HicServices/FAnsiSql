using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Equality;

internal class EqualityTests_ServerAndDatabase
{
    [OneTimeSetUp]
    public void SetUp()
    {
    }

    [TestCase("Server=fish fish fish", DatabaseType.MicrosoftSQLServer,"Server=fish fish fish", DatabaseType.MicrosoftSQLServer)]
    [TestCase(null, DatabaseType.MicrosoftSQLServer, null, DatabaseType.MicrosoftSQLServer)]
    [TestCase(null, DatabaseType.MicrosoftSQLServer, "", DatabaseType.MicrosoftSQLServer)]
    public void EqualityTest_DiscoveredServer_AreEqual(string constr1, DatabaseType type1,string constr2, DatabaseType type2)
    {
        var s1 = new DiscoveredServer(constr1, type1);
        var s2 = new DiscoveredServer(constr2, type2);

        Assert.Multiple(() =>
        {
            Assert.That(s2, Is.EqualTo(s1));
            Assert.That(s2.GetHashCode(), Is.EqualTo(s1.GetHashCode()));

            Assert.That(s2.ExpectDatabase("MyDb"), Is.EqualTo(s1.ExpectDatabase("MyDb")));
            Assert.That(s2.ExpectDatabase("MyDb").GetHashCode(), Is.EqualTo(s1.ExpectDatabase("MyDb").GetHashCode()));

            Assert.That(s2.ExpectDatabase("MyDb"), Is.EqualTo(s1.ExpectDatabase("Mydb")));
            Assert.That(s2.ExpectDatabase("MyDb").GetHashCode(), Is.EqualTo(s1.ExpectDatabase("Mydb").GetHashCode()));

            Assert.That(s2.ExpectDatabase("MyDb2"), Is.Not.EqualTo(s1.ExpectDatabase("MyDb")));
        });

        //This does not affect things since we are expecting a specific database anyway
        s1.ChangeDatabase("Dave");
        Assert.That(s2, Is.Not.EqualTo(s1));
        Assert.That(s2.ExpectDatabase("MyDb"), Is.EqualTo(s1.ExpectDatabase("MyDb")));
        Assert.That(s2.ExpectDatabase("MyDb").GetHashCode(), Is.EqualTo(s1.ExpectDatabase("MyDb").GetHashCode()));

    }

    [TestCase("Server=fish fish fish", DatabaseType.MicrosoftSQLServer, "Server=fish fish fish;Integrated Security=true", DatabaseType.MicrosoftSQLServer)]
    [TestCase("Server=fish fish fish", DatabaseType.MicrosoftSQLServer, "Server=fish fish fish", DatabaseType.MySql)]
    [TestCase(null, DatabaseType.MicrosoftSQLServer, "", DatabaseType.MySql)]
    public void EqualityTest_DiscoveredServer_AreNotEqual(string constr1, DatabaseType type1, string constr2, DatabaseType type2)
    {
        var s1 = new DiscoveredServer(constr1, type1);
        var s2 = new DiscoveredServer(constr2, type2);

        Assert.That(s2, Is.Not.EqualTo(s1));
        Assert.That(s2.ExpectDatabase("MyDb"), Is.Not.EqualTo(s1.ExpectDatabase("MyDb")));
    }
}