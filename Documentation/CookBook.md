# Cook Book

1. [Table Creation](#table-creation)
  1. [Fixed data type](#fixed-data-type)
  1. [Primary Key](#primary-key)
  1. [Nullability / Collation etc](#nullability-collation)

## Table Creation

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
