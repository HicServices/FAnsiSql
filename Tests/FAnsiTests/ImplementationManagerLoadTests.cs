using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests;

internal class ImplementationManagerLoadTests
{
    [Test]
    public void Test_LoadAssemblies_FromDirectory()
    {
        Assert.That(ImplementationManager.GetImplementations(), Has.Count.GreaterThanOrEqualTo(3));
    }
}