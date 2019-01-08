using System;
using System.Data;
using System.Data.Common;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FansiTests
{
    class TestExamples : DatabaseTests
    {
        [Test]
        [Ignore("Test only works when the hard coded connection strings pass,  this test is used to build clear examples in the documentation")]
        public void Example()
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
