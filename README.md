# FAnsiSql
## Purpose
FAnsiSql is a database management/ETL library that allows you to perform common SQL operations without having to know which Database Management System (DBMS) you are targetting (e.g. Sql Server, My Sql, Oracle).  

FAnsiSql is not an ORM API, it deals only in raw data (Strings, `System.DataTable`, Value Types, SQL etc).

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

We have to change the table qualifier, we don't specify schema (dbo) and even the data types are different.  The more advanced the feature, the more desperate the varied the implementations are (e.g. [TOP X](https://www.w3schools.com/sql/sql_top.asp), [UPDATE from JOIN](https://stackoverflow.com/a/1293347/4824531) etc).

The goal of FAnsiSql is to abstract away cross DBMS differences and streamline common tasks while still allowing you to harness the power of executing raw SQL commands.

## Features

Feature Completeness by DBMS:

- [Microsoft Sql](./Implementations/FAnsi.Implementations.MicrosoftSQL/README.md) 
- [MySql](./Implementations/FAnsi.Implementations.MySql/README.md)
- [Oracle](./Implementations/FAnsi.Implementations.Oracle/README.md)

FAnsiSql is built using core `System.Data.Common` classes (e.g. `DBCommand`, `DBConnection` etc).  A common set of operations is defined (See links above) with DBMS specific implementations defined in a separate assemblies (e.g. FAnsi.Implementations.MicrosoftSQL.dll).  Each implementation uses it's own backing library (e.g. [ODP.net](https://www.oracle.com/technetwork/topics/dotnet/index-085163.html) for Oracle).  Implementations are loaded using [Managed Extensibility Framework](https://docs.microsoft.com/en-us/dotnet/framework/mef/).

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
	DbCommand cmd = server.GetCommand("Select * from " + table.GetFullyQualifiedName(), con);
	DbDataReader r = cmd.ExecuteReader();

	while (r.Read())
		Console.WriteLine(string.Join(",", r["Name"],r["DateOfBirth"]));
}
```
