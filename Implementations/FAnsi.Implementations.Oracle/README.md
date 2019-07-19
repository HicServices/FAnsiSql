# Oracle FAnsi Implementation

# Feature Completeness

- Server
  - [X] Check Exists
  - [ ] Describe 
  - [X] List Databases

- Database
  - [X] Create
  - [X] Drop
  - [ ] Backup
  - [ ] Detach
  - [X] List Tables
  - [ ] List Table Valued Functions
  - [ ] List Stored Proceedures
  
- Table 
  - [X] Create
  - [X] Drop
  - [X] Script Table Structure
  - [ ] MakeDistinct
  - [X] Bulk Insert
  - [X] Rename
  - [X] List Foreign Keys
  
- Column
  - [X] Alter
  
- Data Types
  - [X] [Translation](./../../Documentation/TypeTranslation.md)
  - [ ] Bit
  - [X] String
  - [ ] TimeSpan
  - [X] Decimal
  - [X] Date
  - [X] Auto Increment (Requires Oracle 12c)

- Query
  - [X] Top X (Requires Oracle 12c)
  - [X] JOIN UPDATE
  
- Aggregation 
  - [X] Basic GROUP BY
  - [X] Calendar/Axis Table GROUP BY
  - [ ] Dynamic Pivot GROUP BY
  

##Issues
Oracle [does not have a bit data type](https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:6263249199595#876972400346931526).  If you ask to create a bool column you will get a varchar2(5)

```csharp
DiscoveredDatabase db = GetTestDatabase(DatabaseType.Oracle);
DiscoveredTable table = db.CreateTable("MyTable",
	new[]
	{
		new DatabaseColumnRequest("MyCol", new DatabaseTypeRequest(typeof(bool)))
	});

var col = table.DiscoverColumn("MyCol");
Assert.AreEqual("varchar2(5)", col.DataType.SQLType);
```

Oracle does not have a discrete time datatype.  both [date and timestamp](https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT413) store full date/times.  It is possible to use `interval` for this purpose but that type is very flexible (which isn't a problem for creating the column but it is a problem for discovering a column and making a descision about whether it is TimeSpan or DateTime).



