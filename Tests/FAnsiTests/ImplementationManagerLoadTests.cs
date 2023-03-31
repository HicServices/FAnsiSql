using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using NUnit.Framework;
using System.IO;

namespace FAnsiTests;

internal class ImplementationManagerLoadTests
{
    [Test]
    public void Test_LoadAssemblies_FromDirectory()
    {
        ImplementationManager.Clear();
        Assert.IsNull(ImplementationManager.GetImplementations());
        ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));
        Assert.GreaterOrEqual(ImplementationManager.GetImplementations().Count,3);
    }
    [Test]
    public void Test_LoadAssemblies_OneAfterAnother()
    {
        ImplementationManager.Clear();
        Assert.IsNull(ImplementationManager.GetImplementations());

        ImplementationManager.Load<MicrosoftSQLImplementation>();
        Assert.AreEqual(ImplementationManager.GetImplementations().Count,1);

        ImplementationManager.Load<OracleImplementation>();
        Assert.AreEqual(ImplementationManager.GetImplementations().Count,2);

        //repeat loading shouldn't increase the count
        ImplementationManager.Load<OracleImplementation>();
        Assert.AreEqual(ImplementationManager.GetImplementations().Count,2);

        ImplementationManager.Load<MySqlImplementation>();
        Assert.AreEqual(ImplementationManager.GetImplementations().Count,3);
    }
}