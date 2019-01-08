# Cook Book

1. [Table Creation](#table-creation)
   1. [Fixed data type](#fixed-data-type)
   1. [Primary Key](#primary-key)
   1. [Nullability / Collation etc](#nullability-collation)
2. [Bulk Insert](#bulk-insert)

## Table Creation

You can find worked examples of table creation in [_CreateTableTests.cs_](./../Tests/FAnsiTests/Table/CreateTableTests.cs)

### Fixed Data Type

By default the column data type will be computed from the data being uploaded.  If you don't have any data or you want to override one (or more) column data types you can specify this in `DiscoveredDatabase.CreateTable`.

You can either pass a specific proprietary data type:

```csharp
DiscoveredDatabase database = GetTestDatabase(DatabaseType.Oracle);

DiscoveredTable table = database.CreateTable(
	"MyTable",
	new [] {new DatabaseColumnRequest("Name", "VARCHAR2(100)")}
);

```

Or you can specify the Type in C# terms with a `DatabaseTypeRequest` (this has the advantange of being cross DBMS compatible):
```csharp
DiscoveredDatabase database = GetTestDatabase(DatabaseType.Oracle);

DiscoveredTable table = database.CreateTable(
	"MyTable",
	new [] {new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string),10))}
);
```

Test(s): _Test_CreateTable_ProprietaryType_ , _TestTableCreation_

### Primary Key
A primary key will be created on your table if you have one declared on the `System.DataTable` you are uploading:

```csharp
DiscoveredDatabase database = GetTestDatabase(databaseType);

var dt = new DataTable();
dt.Columns.Add("Name");
dt.PrimaryKey = new[] {dt.Columns[0]};
dt.Rows.Add("Frank");

DiscoveredTable table = database.CreateTable("PkTable",dt);

Assert.IsTrue(table.DiscoverColumn("Name").IsPrimaryKey);
```


Or you can specify a primary key as part of a `DatabaseColumnRequest`

```csharp

DiscoveredDatabase database = GetTestDatabase(databaseType);

DiscoveredTable table = database.CreateTable(
	"PkTable",
	new DatabaseColumnRequest[]
	{
		new DatabaseColumnRequest("Name",new DatabaseTypeRequest(typeof(string),10))
			{
				IsPrimaryKey = true
				//todo : other column attributes can be set here
			}
	});
```

### Nullability Collation

Nullability, Collation (if your DBMS supports it) etc can be set when you create a `DatabaseColumnRequest` in the same way as [IsPrimaryKey](#primary-key)

Test(s): _CreateTable_CollationTest_

## Bulk Insert

Each DBMS offers a different class/strategy for high speed inserts into a table.  These are abstracted through the `IBulkCopy` interface.  The basic pattern is as follows:

```csharp
DiscoveredDatabase db = GetTestDatabase(type);

DiscoveredTable tbl = db.CreateTable("MyBulkInsertTest",
	new[]
	{
		new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
		new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
	});


Assert.AreEqual(0, tbl.GetRowCount());

var dt = new DataTable();
dt.Columns.Add("Name");
dt.Columns.Add("Age");
dt.Rows.Add("Dave", 50);
dt.Rows.Add("Jamie",60);

using (IBulkCopy bulk = tbl.BeginBulkInsert())
{
	bulk.Timeout = 30;
	bulk.Upload(dt);

	Assert.AreEqual(2,tbl.GetRowCount());

	dt.Rows.Clear();
	dt.Rows.Add("Frank", 100);
	
	bulk.Upload(dt);

	Assert.AreEqual(3, tbl.GetRowCount());
}
```

The underlying implementation must adhere to the following rules

- Must support transactions
- Must respect table schema (primary keys etc)
- Must not require special priveleges (e.g. LOAD DATA INFILE)

If this requires writing a suboptimal solution (e.g. [extended INSERT](#https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html#option_mysqldump_extended-insert) or [array bound parameters](https://blogs.oracle.com/oraclemagazine/put-your-arrays-in-a-bind)) then this has been implemented.


Tests: [_BulkInsertTest.cs_](./../Tests/FAnsiTests/Table/BulkInsertTest.cs)