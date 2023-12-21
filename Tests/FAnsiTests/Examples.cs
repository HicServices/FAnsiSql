using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests;

internal sealed class TestExamples : DatabaseTests
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

        Assert.Multiple(() =>
        {
            //Database types are compatible with all the data
            Assert.That(table.DiscoverColumn("Date of Birth").DataType.SQLType, Is.EqualTo("datetime2"));
            Assert.That(table.DiscoverColumn("Name").DataType.SQLType, Is.EqualTo("varchar(25)"));

            //And the (string) data is now properly typed and sat in our DBMS
            Assert.That(table.GetRowCount(), Is.EqualTo(2));
            Assert.That(table.GetDataTable().Rows[0][1], Is.EqualTo(new DateTime(1920, 1, 1)));
            Assert.That(table.GetDataTable().Rows[1][1], Is.EqualTo(new DateTime(1910, 5, 22)));
        });
    }

    [Test]
    [Ignore("Test only works when the hard coded connection strings pass,  this test is used to build clear examples in the documentation")]
    public void Example_TableCreation()
    {
        using var dt = new DataTable();

        dt.Columns.Add("Name");
        dt.Columns.Add("DateOfBirth");

        dt.Rows.Add("Frank","2001-01-01");
        dt.Rows.Add("Dave", "2001-01-01");

        //Create a server object
        //var server = new DiscoveredServer(@"server=localhost\sqlexpress;Trusted_Connection=True;", DatabaseType.MicrosoftSQLServer);
        var server = new DiscoveredServer(@"Server=localhost;Uid=root;Pwd=zombie;SSLMode=None", DatabaseType.MySql);

        //Find the database
        var database = server.ExpectDatabase("FAnsiTests");

        //Or create it
        if(!database.Exists())
            database.Create();

        //Create a table that can store the data in dt
        var table = database.CreateTable("MyTable", dt);

        //Table has 2 rows in it
        TestContext.WriteLine("Table {0} has {1} rows" ,table.GetFullyQualifiedName(), table.GetRowCount());
        TestContext.WriteLine("Column Name is of type {0}", table.DiscoverColumn("Name").DataType.SQLType);
        TestContext.WriteLine("Column DateOfBirth is of type {0}", table.DiscoverColumn("DateOfBirth").DataType.SQLType);

        using var con = server.GetConnection();
        con.Open();
        using var cmd = server.GetCommand($"Select * from {table.GetFullyQualifiedName()}", con);
        using var r = cmd.ExecuteReader();
        while (r.Read())
            TestContext.WriteLine(string.Join(",", r["Name"],r["DateOfBirth"]));

        //Drop the table afterwards
        table.Drop();
    }
}