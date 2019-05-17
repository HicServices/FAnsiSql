using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Server
{
    class ServerLevelTests:DatabaseTests
    {
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        public void Server_Exists(DatabaseType type)
        {
            var server = GetTestServer(type);
            Assert.IsTrue(server.Exists(), "Server " + server + " did not exist");
        }

        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        public void Server_RespondsWithinTime(DatabaseType type)
        {
            var server = GetTestServer(type);

            Assert.IsTrue(server.RespondsWithinTime(3,out Exception ex));
        }

        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        public void Server_Helper_GetConnectionStringBuilder(DatabaseType type)
        {
            var server = GetTestServer(type);
                       
            var builder = server.Helper.GetConnectionStringBuilder("loco","bob","franko","wacky");

            var server2 = new DiscoveredServer(builder);

            Assert.AreEqual(server2.Name,"loco");
            Assert.AreEqual(server2.GetCurrentDatabase().GetRuntimeName(),"bob");
            Assert.AreEqual(server2.ExplicitUsernameIfAny,"franko");
            Assert.AreEqual(server2.ExplicitPasswordIfAny,"wacky");
        }


        [TestCase(DatabaseType.MicrosoftSQLServer,DatabaseType.MySql)]
        [TestCase(DatabaseType.MySql, DatabaseType.MicrosoftSQLServer)]
        public void MoveData_BetweenServerTypes(DatabaseType from, DatabaseType to)
        {
            //Create some test data
            DataTable dtToMove = new DataTable();
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
            string sql = tblFrom.ScriptTableCreation(false, false, false, toTable);
            
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

        
    }
}
