using System;
using System.Data;
using System.Data.Common;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using NUnit.Framework;

namespace FAnsiTests
{
    class TestExamples : DatabaseTests
    { 
        [Ignore("Test only works when the hard coded connection strings pass,  this test is used to build clear examples in the documentation")]
        [Test] 
        public void Simple_Example()
        {
            //Some data we want to load
            var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Columns.Add("Date of Birth");
            dt.Rows.Add("Frank \"The Boss\" Spagetti","1920-01-01");
            dt.Rows.Add("Pete Mudarillo","22-May-1910");

            //Load the DBMS implementation(s) you need
            ImplementationManager.Load<MicrosoftSQLImplementation>();

            //Get Management object for the database
            var server = new DiscoveredServer(
                @"server=localhost\sqlexpress;Trusted_Connection=True;", DatabaseType.MicrosoftSQLServer);
            var database = server.ExpectDatabase("test");
            var table = database.ExpectTable("MyTable");
            
            //Throw out whatever was there before
            if(table.Exists())
                table.Drop();

            //Create the table
            database.CreateTable("MyTable",dt);
            
            //Database types are compatible with all the data
            Assert.AreEqual("datetime2",table.DiscoverColumn("Date of Birth").DataType.SQLType);
            Assert.AreEqual("varchar(25)",table.DiscoverColumn("Name").DataType.SQLType);

            //And the (string) data is now properly typed and sat in our DBMS
            Assert.AreEqual(2,table.GetRowCount());    
            Assert.AreEqual(new DateTime(1920,1,1),table.GetDataTable().Rows[0][1]);
            Assert.AreEqual(new DateTime(1910,5,22),table.GetDataTable().Rows[1][1]);
        }

        [Test]
        [Ignore("Test only works when the hard coded connection strings pass,  this test is used to build clear examples in the documentation")]
        public void Example_TableCreation()
        {
            //Load implementation assemblies that are relevant to your application
            ImplementationManager.Load(
                typeof(FAnsi.Implementations.MicrosoftSQL.MicrosoftSQLImplementation).Assembly,
                typeof(FAnsi.Implementations.Oracle.OracleImplementation).Assembly,
                typeof(FAnsi.Implementations.MySql.MySqlImplementation).Assembly);

            //Create some test data
            DataTable dt = new DataTable();

            dt.Columns.Add("Name");
            dt.Columns.Add("DateOfBirth");

            dt.Rows.Add("Frank","2001-01-01");
            dt.Rows.Add("Dave", "2001-01-01");

            //Create a server object
            //var server = new DiscoveredServer(@"server=localhost\sqlexpress;Trusted_Connection=True;", DatabaseType.MicrosoftSQLServer);
            var server = new DiscoveredServer(@"Server=localhost;Uid=root;Pwd=zombie;SSL-Mode=None", DatabaseType.MySql);

            //Find the database
            var database = server.ExpectDatabase("FAnsiTests");
            
            //Or create it
            if(!database.Exists())
                database.Create();

            //Create a table that can store the data in dt
            var table = database.CreateTable("MyTable", dt);

            //Table has 2 rows in it
            Console.WriteLine("Table {0} has {1} rows" ,table.GetFullyQualifiedName(), table.GetRowCount());
            Console.WriteLine("Column Name is of type {0}", table.DiscoverColumn("Name").DataType.SQLType);
            Console.WriteLine("Column DateOfBirth is of type {0}", table.DiscoverColumn("DateOfBirth").DataType.SQLType);

            using (DbConnection con = server.GetConnection())
            {
                con.Open();
                DbCommand cmd = server.GetCommand("Select * from " + table.GetFullyQualifiedName(), con);
                DbDataReader r = cmd.ExecuteReader();

                while (r.Read())
                    Console.WriteLine(string.Join(",", r["Name"],r["DateOfBirth"]));
            }

            //Drop the table afterwards
            table.Drop();
        }
    }
}
