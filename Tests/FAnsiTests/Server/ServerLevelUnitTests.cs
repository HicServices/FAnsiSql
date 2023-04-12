using Microsoft.Data.SqlClient;
using NUnit.Framework;

namespace FAnsiTests.Server;

internal class ServerLevelUnitTests
{
    [Test]
    public void ConstructionStringBuilderTest()
    {
        var b = new SqlConnectionStringBuilder("Server=localhost;Database=RDMP_Catalogue;User ID=SA;Password=blah;Trust Server Certificate=true;Encrypt=True")
            {
                InitialCatalog = "master"
            };

        Assert.AreEqual("Data Source=localhost;Initial Catalog=master;User ID=SA;Password=blah;Encrypt=True;Trust Server Certificate=True", b.ConnectionString);
    }
}