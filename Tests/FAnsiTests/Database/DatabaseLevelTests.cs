using System;
using System.Collections;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using Oracle.ManagedDataAccess.Client;
using TypeGuesser;

namespace FAnsiTests.Database;

internal sealed class DatabaseLevelTests : DatabaseTests
{
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Database_Exists(DatabaseType type)
    {
        var server = GetTestDatabase(type);
        Assert.That(server.Exists(), "Server " + server + " did not exist");
    }


    [TestCase(DatabaseType.MySql, false)]
    [TestCase(DatabaseType.MicrosoftSQLServer, false)]
    [TestCase(DatabaseType.Oracle, true)]
    [TestCase(DatabaseType.PostgreSql, false)]
    public void Test_ExpectDatabase(DatabaseType type, bool upperCase)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var server = new DiscoveredServer(helper.GetConnectionStringBuilder("loco", "db", "frank", "kangaro"));
        var db = server.ExpectDatabase("omg");
        Assert.That(db.GetRuntimeName(), Is.EqualTo(upperCase ? "OMG" : "omg"));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_CreateSchema(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        Assert.DoesNotThrow(() => db.CreateSchema("Fr ank"));
        Assert.DoesNotThrow(() => db.CreateSchema("Fr ank"));

        db.Server.GetQuerySyntaxHelper().EnsureWrapped("Fr ank");

        if (type is not (DatabaseType.MicrosoftSQLServer or DatabaseType.PostgreSql)) return;

        var tbl = db.CreateTable("Heyyy",
            [new DatabaseColumnRequest("fff", new DatabaseTypeRequest(typeof(string), 10))], "Fr ank");

        Assert.That(tbl.Exists());

        if (type == DatabaseType.MicrosoftSQLServer)
            Assert.That(tbl.Schema, Is.EqualTo("Fr ank"));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void TestListDatabasesAsync(DatabaseType type)
    {
        var db = GetTestDatabase(type, false);

        Constraint exceptionType = type switch
        {
            DatabaseType.MySql => Throws.TypeOf<OperationCanceledException>(),
            DatabaseType.MicrosoftSQLServer => Throws.TypeOf<TaskCanceledException>(),
            DatabaseType.PostgreSql => Throws.Nothing,
            DatabaseType.Oracle => Throws.TypeOf<OracleException>(),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
        };

        Assert.That(
            () => db.Server.Helper.ListDatabasesAsync(db.Server.Builder, new CancellationToken(true))
                .ToBlockingEnumerable().ToList(), exceptionType);
        var databases = db.Server.Helper.ListDatabasesAsync(db.Server.Builder, CancellationToken.None).ToBlockingEnumerable().ToList();
        Assert.That(databases, Has.Member(db.GetRuntimeName()).Using((IEqualityComparer)StringComparer.OrdinalIgnoreCase));
    }
}