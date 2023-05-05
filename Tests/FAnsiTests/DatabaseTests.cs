using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Implementation;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.PostgreSql;
using NUnit.Framework;

namespace FAnsiTests;

[SingleThreaded]
[NonParallelizable]
public class DatabaseTests
{
    protected readonly Dictionary<DatabaseType,string> TestConnectionStrings = new();

    private bool _allowDatabaseCreation;
    private string _testScratchDatabase;

    private const string TestFilename = "TestDatabases.xml";

    [OneTimeSetUp]
    public void CheckFiles()
    {
        try
        {
            ImplementationManager.Load<OracleImplementation>();
            ImplementationManager.Load<MicrosoftSQLImplementation>();
            ImplementationManager.Load<MySqlImplementation>();
            ImplementationManager.Load<PostgreSqlImplementation>();

            var file = Path.Combine(TestContext.CurrentContext.TestDirectory, TestFilename);
            
            Assert.IsTrue(File.Exists(file),"Could not find " + TestFilename);

            var doc = XDocument.Load(file);

            var root = doc.Element("TestDatabases")??throw new Exception($"Missing element 'TestDatabases' in {TestFilename}");

            var settings = root.Element("Settings") ??
                           throw new Exception($"Missing element 'Settings' in {TestFilename}");

            var e = settings.Element("AllowDatabaseCreation") ??
                    throw new Exception($"Missing element 'AllowDatabaseCreation' in {TestFilename}");

            _allowDatabaseCreation = Convert.ToBoolean(e.Value);

            e = settings.Element("TestScratchDatabase") ??
                throw new Exception($"Missing element 'TestScratchDatabase' in {TestFilename}");

            _testScratchDatabase = e.Value;
            
            foreach (var element in root.Elements("TestDatabase"))
            {
                var type = element.Element("DatabaseType")?.Value;

                if(!Enum.TryParse(type, out DatabaseType databaseType))
                    throw new Exception($"Could not parse DatabaseType {type}");

         
                var constr = element.Element("ConnectionString")?.Value;
                
                TestConnectionStrings.Add(databaseType,constr);
            }
        }
        catch (Exception exception)
        {
            TestContext.WriteLine(exception);
            throw;
        }
            
    }

    protected IEnumerable<DiscoveredServer> TestServer()
    {
        return TestConnectionStrings.Select(kvp => new DiscoveredServer(kvp.Value, kvp.Key));
    }
    protected DiscoveredServer GetTestServer(DatabaseType type)
    {
        if(!TestConnectionStrings.ContainsKey(type))
            Assert.Inconclusive("No connection string configured for that server");

        return new DiscoveredServer(TestConnectionStrings[type], type);
    }

    protected DiscoveredDatabase GetTestDatabase(DatabaseType type, bool cleanDatabase=true)
    {
        var server = GetTestServer(type);
        var db = server.ExpectDatabase(_testScratchDatabase);

        if(!db.Exists())
            if(_allowDatabaseCreation)
                db.Create();
            else
            {
                Assert.Inconclusive(
                    $"Database {_testScratchDatabase} did not exist on server {server} and AllowDatabaseCreation was false in {TestFilename}");
            }
        else
        {
            if (!cleanDatabase) return db;
            IEnumerable<DiscoveredTable> deleteTableOrder;

            try
            {
                //delete in reverse dependency order to avoid foreign key constraint issues preventing deleting
                var tree = new RelationshipTopologicalSort(db.DiscoverTables(true));
                deleteTableOrder = tree.Order.Reverse();
            }
            catch (FAnsi.Exceptions.CircularDependencyException)
            {
                deleteTableOrder = db.DiscoverTables(true);
            }

            foreach (var t in deleteTableOrder)
                t.Drop();

            foreach (var func in db.DiscoverTableValuedFunctions())
                func.Drop();
        }

        return db;
    }

    protected void AssertCanCreateDatabases()
    {
        if(!_allowDatabaseCreation)
            Assert.Inconclusive("Test cannot run when AllowDatabaseCreation is false");
    }

    private static bool AreBasicallyEquals(object o, object o2, bool handleSlashRSlashN = true)
    {
        //if they are legit equals
        if (Equals(o, o2))
            return true;

        //if they are null but basically the same
        var oIsNull = o == null || o == DBNull.Value || o.ToString()?.Equals("0")==true;
        var o2IsNull = o2 == null || o2 == DBNull.Value || o2.ToString()?.Equals("0")==true;

        if (oIsNull || o2IsNull)
            return oIsNull == o2IsNull;

        //they are not null so tostring them deals with int vs long etc that DbDataAdapters can be a bit flaky on
        if (handleSlashRSlashN)
            return string.Equals(o.ToString()?.Replace("\r", "").Replace("\n", ""), o2.ToString()?.Replace("\r", "").Replace("\n", ""));

        return string.Equals(o.ToString(), o2.ToString());
    }

    protected static void AssertAreEqual(DataTable dt1, DataTable dt2)
    {
        Assert.AreEqual(dt1.Columns.Count, dt2.Columns.Count, "DataTables had a column count mismatch");
        Assert.AreEqual(dt1.Rows.Count, dt2.Rows.Count, "DataTables had a row count mismatch");

        foreach (DataRow row1 in dt1.Rows)
        {
            var match = dt2.Rows.Cast<DataRow>().Any(row2=> dt1.Columns.Cast<DataColumn>().All(c => AreBasicallyEquals(row1[c.ColumnName], row2[c.ColumnName])));
            Assert.IsTrue(match, "Couldn't find match for row:{0}", string.Join(",", row1.ItemArray));
        }

    }
}