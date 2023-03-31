using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using NUnit.Framework;

namespace FAnsiTests.Server;

public class ConnectionStringKeywordAccumulatorTests
{
    private readonly Dictionary<DatabaseType, IDiscoveredServerHelper> helpers = new Dictionary
        <DatabaseType, IDiscoveredServerHelper>()
        {
            {DatabaseType.MicrosoftSQLServer, new MicrosoftSQLServerHelper()},
            {DatabaseType.MySql, new MySqlServerHelper()},
            {DatabaseType.Oracle, new OracleServerHelper()},
            {DatabaseType.PostgreSql, new PostgreSqlServerHelper()},
        };

    [Test]
    public void TestKeywords()
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MySql);
        acc.AddOrUpdateKeyword("Auto Enlist", "false", ConnectionStringKeywordPriority.SystemDefaultLow);
            
        DbConnectionStringBuilder connectionStringBuilder = helpers[DatabaseType.MySql].GetConnectionStringBuilder("localhost","mydb","frank","kangaro");

        StringAssert.DoesNotContain("auto enlist",connectionStringBuilder.ConnectionString);

        acc.EnforceOptions(connectionStringBuilder);

        Assert.IsTrue(connectionStringBuilder.ConnectionString.IndexOf("Auto Enlist=false", StringComparison.InvariantCultureIgnoreCase) != -1);
    }


    [Test]
    public void TestKeywords_OverrideWithLowerPriority_Ignored()
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MicrosoftSQLServer);
        acc.AddOrUpdateKeyword("Pooling", "false", ConnectionStringKeywordPriority.SystemDefaultHigh);

        DbConnectionStringBuilder connectionStringBuilder = helpers[DatabaseType.MicrosoftSQLServer].GetConnectionStringBuilder("localhost", "mydb", "frank","kangaro");

        StringAssert.DoesNotContain("pooling", connectionStringBuilder.ConnectionString);

        acc.EnforceOptions(connectionStringBuilder);

        StringAssert.Contains("Pooling=False", connectionStringBuilder.ConnectionString);

        //attempt override with low priority setting it to true (note we flipped case of P just to be a curve ball)
        acc.AddOrUpdateKeyword("pooling","true",ConnectionStringKeywordPriority.SystemDefaultLow);

        acc.EnforceOptions(connectionStringBuilder);

        StringAssert.Contains("Pooling=False", connectionStringBuilder.ConnectionString);
    }

    [TestCase(DatabaseType.MicrosoftSQLServer, "AttachDbFilename", @"c:\temp\db", "Initial File Name", @"x:\omg.mdf")]
    [TestCase(DatabaseType.Oracle, "CONNECTION TIMEOUT", "10", "Connection Timeout", "20")]
    [TestCase(DatabaseType.PostgreSql, "Database", "mydb", "DATABASE", "myotherdb")]
    public void TestKeywords_OverrideWithNovelButEquivalentKeyword_Ignored(DatabaseType databaseType, string key1, string value1, string equivalentKey, string value2)
    {
        var acc = new ConnectionStringKeywordAccumulator(databaseType);
        acc.AddOrUpdateKeyword(key1,value1, ConnectionStringKeywordPriority.SystemDefaultHigh);

        DbConnectionStringBuilder connectionStringBuilder = helpers[databaseType].GetConnectionStringBuilder("localhost", "mydb", "frank","kangaro");

        acc.EnforceOptions(connectionStringBuilder);

        StringAssert.Contains(key1 + "=" + value1, connectionStringBuilder.ConnectionString);
            
        //attempt override with low priority setting it to true but also use the alias
        acc.AddOrUpdateKeyword(equivalentKey,value2,ConnectionStringKeywordPriority.SystemDefaultLow);

        acc.EnforceOptions(connectionStringBuilder);

        StringAssert.Contains(key1 + "=" + value1, connectionStringBuilder.ConnectionString, "ConnectionStringKeywordAccumulator did not realise that keywords are equivalent");
    }
    [TestCase(ConnectionStringKeywordPriority.SystemDefaultHigh)] //same as current (still results in override)
    [TestCase(ConnectionStringKeywordPriority.ApiRule)]
    public void TestKeywords_OverrideWithHigherPriority_Respected(ConnectionStringKeywordPriority newPriority)
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MicrosoftSQLServer);
        acc.AddOrUpdateKeyword("Pooling", "false", ConnectionStringKeywordPriority.SystemDefaultHigh);

        DbConnectionStringBuilder connectionStringBuilder = helpers[DatabaseType.MicrosoftSQLServer].GetConnectionStringBuilder("localhost", "mydb", "frank","kangaro");

        StringAssert.DoesNotContain("pooling", connectionStringBuilder.ConnectionString);

        acc.EnforceOptions(connectionStringBuilder);

        StringAssert.Contains("Pooling=False", connectionStringBuilder.ConnectionString);

        //attempt override with low priority setting it to true (note we flipped case of P just to be a curve ball)
        acc.AddOrUpdateKeyword("pooling", "true", newPriority);

        acc.EnforceOptions(connectionStringBuilder);

        StringAssert.Contains("Pooling=True", connectionStringBuilder.ConnectionString);
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestKeywords_Invalid(DatabaseType databaseType)
    {
        var acc = new ConnectionStringKeywordAccumulator(databaseType);
          
        var ex = Assert.Throws<ArgumentException>(()=>acc.AddOrUpdateKeyword("FLIBBLE", "false", ConnectionStringKeywordPriority.SystemDefaultLow));

        StringAssert.Contains("FLIBBLE",ex.Message);
    }
}