using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests.Server;

internal class ServerLevelTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Server_Exists(DatabaseType type)
    {
        var server = GetTestServer(type);
        Assert.IsTrue(server.Exists(), "Server " + server + " did not exist");
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Server_Constructors(DatabaseType dbType)
    {
        var helper = ImplementationManager.GetImplementation(dbType).GetServerHelper();
        var server = new DiscoveredServer(helper.GetConnectionStringBuilder("localhost", null,"franko","wacky").ConnectionString,dbType);

        Assert.AreEqual("localhost",server.Name);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Server_RespondsWithinTime(DatabaseType type)
    {
        var server = GetTestServer(type);

        Assert.IsTrue(server.RespondsWithinTime(3,out _));
    }

    /// <summary>
    /// Tests systems ability to deal with missing information in the connection string
    /// </summary>
    /// <param name="type"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void ServerHelper_GetCurrentDatabase_WhenNoneSpecified(DatabaseType type)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();            
        var builder = helper.GetConnectionStringBuilder("");
        var server = new DiscoveredServer(builder);

        Assert.AreEqual(null,server.Name);
        Assert.AreEqual(null,server.GetCurrentDatabase());
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void ServerHelper_GetConnectionStringBuilder(DatabaseType type)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var builder = helper.GetConnectionStringBuilder("loco","bob","franko","wacky");

        var server = new DiscoveredServer(builder);

        Assert.AreEqual("loco",server.Name);

        //Oracle does not persist database in connection string
        if(type == DatabaseType.Oracle)
            Assert.IsNull(server.GetCurrentDatabase());
        else
            Assert.AreEqual("bob",server.GetCurrentDatabase().GetRuntimeName());

        Assert.AreEqual("franko",server.ExplicitUsernameIfAny);
        Assert.AreEqual("wacky",server.ExplicitPasswordIfAny);
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void ServerHelper_GetConnectionStringBuilder_NoDatabase(DatabaseType type,bool useWhitespace)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var builder = helper.GetConnectionStringBuilder("loco",useWhitespace? "  ":null,"franko","wacky");

        var server = new DiscoveredServer(builder);

        Assert.AreEqual("loco",server.Name);

        Assert.IsNull(server.GetCurrentDatabase());

        Assert.AreEqual("franko",server.ExplicitUsernameIfAny);
        Assert.AreEqual("wacky",server.ExplicitPasswordIfAny);

        server = new DiscoveredServer("loco",useWhitespace?"  ":null,type,"frank","kangaro");
        Assert.AreEqual("loco",server.Name);

        Assert.IsNull(server.GetCurrentDatabase());


    }

    [TestCase(DatabaseType.MySql,false)]
    [TestCase(DatabaseType.MicrosoftSQLServer,false)]
    [TestCase(DatabaseType.Oracle,true)]
    [TestCase(DatabaseType.PostgreSql,false)]
    public void ServerHelper_ChangeDatabase(DatabaseType type,bool expectCaps)
    {
        var server = new DiscoveredServer("loco","bob",type,"franko","wacky");

        Assert.AreEqual("loco",server.Name);
            
        //this failure is already exposed by Server_Helper_GetConnectionStringBuilder
        Assert.AreEqual(expectCaps?"BOB":"bob",server.GetCurrentDatabase().GetRuntimeName());

        Assert.AreEqual("franko",server.ExplicitUsernameIfAny);
        Assert.AreEqual("wacky",server.ExplicitPasswordIfAny);

        server.ChangeDatabase("omgggg");

        Assert.AreEqual(server.Name,"loco");
            
        Assert.AreEqual(expectCaps?"OMGGGG":"omgggg",server.GetCurrentDatabase().GetRuntimeName());
        Assert.AreEqual("franko",server.ExplicitUsernameIfAny);
        Assert.AreEqual("wacky",server.ExplicitPasswordIfAny);
    }


    /// <summary>
    /// Checks the API for <see cref="DiscoveredServer"/> respects both changes using the API and direct user changes made
    /// to <see cref="DiscoveredServer.Builder"/>
    /// </summary>
    /// <param name="type"></param>
    /// <param name="useApiFirst"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void ServerHelper_ChangeDatabase_AdHoc(DatabaseType type, bool useApiFirst)
    {
        if(type == DatabaseType.Oracle)
            Assert.Inconclusive("FAnsiSql understanding of Database cannot be encoded in DbConnectionStringBuilder sadly so we can end up with DiscoveredServer with no GetCurrentDatabase");

        //create initial server reference
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var server = new DiscoveredServer(helper.GetConnectionStringBuilder("loco","bob","franko","wacky"));
        Assert.AreEqual("loco",server.Name);
        Assert.AreEqual("bob",server.GetCurrentDatabase().GetRuntimeName());

        //Use API to change databases
        if(useApiFirst)
        {
            server.ChangeDatabase("omgggg");
            Assert.AreEqual("loco",server.Name);
            Assert.AreEqual("omgggg",server.GetCurrentDatabase().GetRuntimeName());
        }

        //adhoc changes to builder
        server.Builder["Database"] = "Fisss";
        Assert.AreEqual("loco",server.Name);
        Assert.AreEqual("Fisss",server.GetCurrentDatabase().GetRuntimeName());

        server.Builder["Server"] = "Amagad";
        Assert.AreEqual("Amagad",server.Name);
        Assert.AreEqual("Fisss",server.GetCurrentDatabase().GetRuntimeName());
    }

    [TestCase(DatabaseType.MicrosoftSQLServer,DatabaseType.MySql)]
    [TestCase(DatabaseType.MySql, DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MicrosoftSQLServer,DatabaseType.PostgreSql)]
    [TestCase(DatabaseType.PostgreSql, DatabaseType.MicrosoftSQLServer)]

    public void MoveData_BetweenServerTypes(DatabaseType from, DatabaseType to)
    {
        //Create some test data
        var dtToMove = new DataTable();
        dtToMove.Columns.Add("MyCol");
        dtToMove.Columns.Add("DateOfBirth");
        dtToMove.Columns.Add("Sanity");
            
        dtToMove.Rows.Add("Frank",new DateTime(2001,01,01),"0.50");
        dtToMove.Rows.Add("Tony", null,"9.99");
        dtToMove.Rows.Add("Jez", new DateTime(2001, 05, 01),"100.0");

        dtToMove.PrimaryKey = new[] {dtToMove.Columns["MyCol"]};
            
        //Upload it to the first database
        var fromDb = GetTestDatabase(from);
        var tblFrom = fromDb.CreateTable("MyTable", dtToMove);
        Assert.IsTrue(tblFrom.Exists());

        //Get pointer to the second database table (which doesn't exist yet)
        var toDb = GetTestDatabase(to);
        var toTable = toDb.ExpectTable("MyNewTable");
        Assert.IsFalse(toTable.Exists());

        //Get the clone table sql adjusted to work on the other DBMS
        var sql = tblFrom.ScriptTableCreation(false, false, false, toTable);
            
        //open connection and run the code to create the new table
        using(var con = toDb.Server.GetConnection())
        {
            con.Open();
            var cmd = toDb.Server.GetCommand(sql, con);
            cmd.ExecuteNonQuery();
        }

        //new table should exist
        Assert.IsTrue(tblFrom.Exists());

        using (var insert = toTable.BeginBulkInsert())
        {
            //fetch the data from the source table
            var fromData = tblFrom.GetDataTable();

            //put it into the destination table
            insert.Upload(fromData);
        }

        Assert.AreEqual(3, tblFrom.GetRowCount());
        Assert.AreEqual(3, toTable.GetRowCount());

        AssertAreEqual(toTable.GetDataTable(), tblFrom.GetDataTable());
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void TestServer_GetVersion(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType, false);
        var ver = db.Server.GetVersion();

        TestContext.WriteLine($"Version:{ver}");
        Assert.IsNotNull(ver);

        Assert.Greater(ver.Major,0);
    }

}