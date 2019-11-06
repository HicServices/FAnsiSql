# FAnsiSql

[![Build Status](https://travis-ci.org/HicServices/FAnsiSql.svg?branch=master)](https://travis-ci.org/HicServices/FAnsiSql) [![Total alerts](https://img.shields.io/lgtm/alerts/g/HicServices/FAnsiSql.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/HicServices/FAnsiSql/alerts/) [![NuGet Badge](https://buildstats.info/nuget/HIC.FAnsiSql)](https://buildstats.info/nuget/HIC.FAnsiSql)

- [Nuget](https://www.nuget.org/packages/HIC.FansiSql/)
- [Dependencies](./Packages.md)
- [Changelog](./CHANGELOG.md)

<p align="right">
<a href="https://www.publicdomainpictures.net/en/view-image.php?image=184699&picture=a-laugh-every-day-126">
  <img src="FansiHammerSmall.png" align="right"/>
</a>
</p>

Ever had difficulty getting a DataTable into a database? Maybe the dates are going in as strings or some clever dude put spaces in the middle of column names?  FAnsiSql has you covered:


```csharp 
//Some data we want to load
var dt = new DataTable();
dt.Columns.Add("Name");
dt.Columns.Add("Date of Birth");
dt.Rows.Add("Frank \"The Boss\" Spagetti","1920-01-01");
dt.Rows.Add("Pete Mudarillo","22-May-1910");

//Load the DBMS implementation(s) you need
ImplementationManager.Load<MicrosoftSQLImplementation>();

//Get management object for the database
var server = new DiscoveredServer(
    @"server=localhost\sqlexpress;Trusted_Connection=True;",
     DatabaseType.MicrosoftSQLServer);

var database = server.ExpectDatabase("test");
var table = database.ExpectTable("MyTable");
            
//Throw out whatever was there before
if(table.Exists())
    table.Drop();

//Create the table
database.CreateTable("MyTable",dt);
            
//Database types are compatible with all the data
Assert.AreEqual("datetime2",
    table.DiscoverColumn("Date of Birth").DataType.SQLType);

Assert.AreEqual("varchar(25)",
    table.DiscoverColumn("Name").DataType.SQLType);

//And the (string) data is now properly typed and sat in our DBMS
Assert.AreEqual(2,table.GetRowCount());    
Assert.AreEqual(new DateTime(1920,1,1),
    table.GetDataTable().Rows[0][1]);

Assert.AreEqual(new DateTime(1910,5,22),
    table.GetDataTable().Rows[1][1]);
```

FAnsi Sql! it's like a budget version of [SMO](https://docs.microsoft.com/en-us/sql/relational-databases/server-management-objects-smo/sql-server-management-objects-smo-programming-guide?view=sql-server-2017) (that works cross platform - Sql Server,  MySql and Oracle).  It supports:

 * Table Creation
 * Assigning types to untyped (string) data
 * Bulk Insert
 * DDL operations (Create database, drop database etc)
 * Discovery (Does table exist?, what columns are in table? etc)
 * Query writting assistance (e.g. TOP X)

It is **not** an [ORM](https://en.wikipedia.org/wiki/Object-relational_mapping), it deals only in value type data (Strings, `System.DataTable`, Value Types, SQL etc).

## Install

FAnsi Sql is a [nuget package](https://www.nuget.org/packages/HIC.FansiSql/).  You can install it using either using the package manager:

```
PM> Install-Package HIC.FansiSql
```
Or .NET CLI Console:

```
> dotnet add package HIC.FansiSql
```

## Feature Completeness

Most features are implemented across all 4 DBMS, you can find a breakdown of progress here:

- [Microsoft Sql](./Implementations/FAnsi.Implementations.MicrosoftSQL/README.md) 
- [MySql](./Implementations/FAnsi.Implementations.MySql/README.md)
- [Oracle](./Implementations/FAnsi.Implementations.Oracle/README.md)
- [Postgres](./Implementations/FAnsi.Implementations.PostgreSql/README.md)

Implementations are defined in separate assemblies (e.g. FAnsi.Implementations.MicrosoftSQL.dll) to allow for future expansion.  Each implementation uses it's own backing library (e.g. [ODP.net](https://www.oracle.com/technetwork/topics/dotnet/index-085163.html) for Oracle).  Implementations are loaded using [Managed Extensibility Framework](https://docs.microsoft.com/en-us/dotnet/framework/mef/).

## Why is it useful?
FAnsiSql is a database management/ETL library that allows you to perform common SQL operations without having to know which Database Management System (DBMS) you are targetting (e.g. Sql Server, My Sql, Oracle).  

Consider writing an SQL create table command:

```sql
CREATE TABLE table_name (
	column1 datatype,
	column2 datatype,
	column3 datatype,
	....
);
```

If we wanted to target Microsoft Sql Server we might write something like:


```sql
CREATE TABLE [FAnsiTests].[dbo].[MyTable]
(
	Name varchar(10) NULL,
	DateOfBirth datetime2 NULL,
);
```

The same code on MySql would be:

```sql
CREATE TABLE `FAnsiTests`.`MyTable`
(
	`Name` varchar(5) NULL ,
	`DateOfBirth` datetime NULL 
);
```

We have to change the table qualifier, we don't specify schema (dbo) and even the data types are different.  The more advanced the feature, the more disparate the varied the implementations are (e.g. [TOP X](https://www.w3schools.com/sql/sql_top.asp), [UPDATE from JOIN](https://stackoverflow.com/a/1293347/4824531) etc).

The goal of FAnsiSql is to abstract away cross DBMS differences and streamline common tasks while still allowing you to harness the power of executing raw SQL commands.

## Example

Imagine we have a `System.DataTable` in memory and we want to store it in a database with an appropriate schema.

```csharp
//Create some test data
DataTable dt = new DataTable();

dt.Columns.Add("Name");
dt.Columns.Add("DateOfBirth");

dt.Rows.Add("Frank","2001-01-01");
dt.Rows.Add("Dave", "2001-01-01");

//Load implementation assemblies that are relevant to your application  (do this once on startup)
ImplementationManager.Load(
	typeof(FAnsi.Implementations.MicrosoftSQL.MicrosoftSQLImplementation).Assembly,
	typeof(FAnsi.Implementations.MySql.MySqlImplementation).Assembly);

//Create a server object
var server = new DiscoveredServer(@"server=localhost\sqlexpress;Trusted_Connection=True;", DatabaseType.MicrosoftSQLServer);

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

//Drop the table afterwards
table.Drop();
```

This will output the following:

```
Table [FAnsiTests]..[MyTable] has 2 rows
Column Name is of type varchar(5)
Column DateOfBirth is of type datetime2
```

We can get the same code to execute against a MySql server by changing only the connection string line:

```csharp
var server = new DiscoveredServer(@"Server=localhost;Uid=root;Pwd=***;SSL-Mode=None", DatabaseType.MySql);
```

In this case we get the following output:

```
Table `FAnsiTests`.`MyTable` has 2 rows
Column Name is of type varchar(5)
Column DateOfBirth is of type datetime
```

We can still execute raw ANSI Sql against the table

```csharp
using (DbConnection con = server.GetConnection())
{
    con.Open();
    using(DbCommand cmd = server.GetCommand("Select * from " + table.GetFullyQualifiedName(), con))
        using(DbDataReader r = cmd.ExecuteReader())
            while (r.Read())
                Console.WriteLine(string.Join(",", r["Name"],r["DateOfBirth"]));
}
```


## Building

Building requires MSBuild 15 or later (or Visual Studio 2017 or later).  You will also need to install the DotNetCore 2.2 SDK.
