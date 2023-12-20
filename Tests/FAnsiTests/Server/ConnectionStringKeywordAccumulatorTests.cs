using System;
using System.Collections.Generic;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace FAnsiTests.Server;

public class ConnectionStringKeywordAccumulatorTests
{
    private readonly Dictionary<DatabaseType, IDiscoveredServerHelper> _helpers = new()
        {
            {DatabaseType.MicrosoftSQLServer, MicrosoftSQLServerHelper.Instance},
            {DatabaseType.MySql, MySqlServerHelper.Instance},
            {DatabaseType.Oracle, OracleServerHelper.Instance},
            {DatabaseType.PostgreSql, PostgreSqlServerHelper.Instance}
        };

    [Test]
    public void TestKeywords()
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MySql);
        acc.AddOrUpdateKeyword("Auto Enlist", "false", ConnectionStringKeywordPriority.SystemDefaultLow);

        var connectionStringBuilder = _helpers[DatabaseType.MySql].GetConnectionStringBuilder("localhost","mydb","frank","kangaro");

        Assert.That(connectionStringBuilder.ConnectionString, Does.Not.Contain("auto enlist"));

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString.Contains("Auto Enlist=false", StringComparison.InvariantCultureIgnoreCase));
    }


    [Test]
    public void TestKeywords_OverrideWithLowerPriority_Ignored()
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MicrosoftSQLServer);
        acc.AddOrUpdateKeyword("Pooling", "false", ConnectionStringKeywordPriority.SystemDefaultHigh);

        var connectionStringBuilder = _helpers[DatabaseType.MicrosoftSQLServer].GetConnectionStringBuilder("localhost", "mydb", "frank","kangaro");

        Assert.That(connectionStringBuilder.ConnectionString, Does.Not.Contain("pooling"));

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=False"));

        //attempt override with low priority setting it to true (note we flipped case of P just to be a curve ball)
        acc.AddOrUpdateKeyword("pooling","true",ConnectionStringKeywordPriority.SystemDefaultLow);

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=False"));
    }

    [TestCase(DatabaseType.MicrosoftSQLServer, "AttachDbFilename", @"c:\temp\db", "Initial File Name", @"x:\omg.mdf")]
    [TestCase(DatabaseType.Oracle, "CONNECTION TIMEOUT", "10", "Connection Timeout", "20")]
    [TestCase(DatabaseType.PostgreSql, "Database", "mydb", "DATABASE", "myotherdb")]
    public void TestKeywords_OverrideWithNovelButEquivalentKeyword_Ignored(DatabaseType databaseType, string key1, string value1, string equivalentKey, string value2)
    {
        var acc = new ConnectionStringKeywordAccumulator(databaseType);
        acc.AddOrUpdateKeyword(key1,value1, ConnectionStringKeywordPriority.SystemDefaultHigh);

        var connectionStringBuilder = _helpers[databaseType].GetConnectionStringBuilder("localhost", "mydb", "frank","kangaro");

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain($"{key1}={value1}"));

        //attempt override with low priority setting it to true but also use the alias
        acc.AddOrUpdateKeyword(equivalentKey,value2,ConnectionStringKeywordPriority.SystemDefaultLow);

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain($"{key1}={value1}"), "ConnectionStringKeywordAccumulator did not realise that keywords are equivalent");
    }
    [TestCase(ConnectionStringKeywordPriority.SystemDefaultHigh)] //same as current (still results in override)
    [TestCase(ConnectionStringKeywordPriority.ApiRule)]
    public void TestKeywords_OverrideWithHigherPriority_Respected(ConnectionStringKeywordPriority newPriority)
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MicrosoftSQLServer);
        acc.AddOrUpdateKeyword("Pooling", "false", ConnectionStringKeywordPriority.SystemDefaultHigh);

        var connectionStringBuilder = _helpers[DatabaseType.MicrosoftSQLServer].GetConnectionStringBuilder("localhost", "mydb", "frank","kangaro");

        Assert.That(connectionStringBuilder.ConnectionString, Does.Not.Contain("pooling"));

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=False"));

        //attempt override with low priority setting it to true (note we flipped case of P just to be a curve ball)
        acc.AddOrUpdateKeyword("pooling", "true", newPriority);

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=True"));
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestKeywords_Invalid(DatabaseType databaseType)
    {
        var acc = new ConnectionStringKeywordAccumulator(databaseType);

        var ex = Assert.Throws<ArgumentException>(()=>acc.AddOrUpdateKeyword("FLIBBLE", "false", ConnectionStringKeywordPriority.SystemDefaultLow));

        Assert.That(ex?.Message, Does.Contain("FLIBBLE"));
    }
}