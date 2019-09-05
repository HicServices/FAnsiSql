# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.1] - 2019-09-05

### Fixed

- Fixed bug in bulk insert where the uploaded DataTable column Order (DataColumn.Ordinal) would change when creating Hard Typed columns out of untyped string columns.  This bug only manifested if you did operations based on column order on the DataTable after it had been inserted into the database succesfully.
- Fixed bug in DiscoveredTableValuedFunction that prevented dropping if they were not in the default schema "dbo"

## [0.10.0] - 2019-08-30

### Changed

- Type Guessing rules adjusted (and moved to [new repository TypeGuesser](https://github.com/HicServices/TypeGuesser))
  - Bit strings now include "Y", "N" "1" and "0".
  - Zeros after decimal point no longer prohibit guessing int (e.g. 1.00 is the now `int` instead of `decimal(3,2)`) 
- DecimalSize class now uses `int` instead of nullable int (`int?`) for number of digits before/after decimal point.
- Table/column name suggester now allows unicode characters (now called `GetSensibleEntityNameFromString`)
- Attempting to resize a column to the same size it is currently is now ignored (previously `InvalidResizeException` was thrown)
- Added new Exception types (instead of generic .net Exceptions)
  - ColumnMappingException when insert / bulk insert fails to match input columns to destination table
  - TypeNotMappedException when there is a problem translating a C# Type to a proprietary SQL datatype (or vice versa)
- MakeDistinct on DiscoveredTable no longer throws an Exception if the table has a Primary Key (instead the method exits without doing anything)
- Reduced code duplication in AggregateHelper implementations by centralising code in new class AggregateCustomLineCollection

### Fixed

- Fixed support for Unicode in table/column names in Sql Server

## [0.9.8] - 2019-08-26

## Added
- Support for unicode text
- `DecimalTypeDecider` now recognises floating poing notation e.g. "-4.10235746055587E-05"

## [0.9.7] - 2019-08-20

## Added

- Added method `IsValidDatabaseName` (and table/column variants) to `QuerySyntaxHelper`.  This allows testing strings without try/catch

### Fixed

- Tables with invalid names e.g. `[mytbl.lol]][.lol.lol]` are no longer returned by `DiscoveredDatabase.DiscoverTables` (previously a `RuntimeNameException` was thrown)

## [0.9.6] - 2019-08-09

### Fixed

- Improved error messages in Sql Server for failed bulk insert
- Reduced MaximumDatabaseLength in Sql Server to 100 (previously 124) to allow for longer default log file suffixes
 

## [0.9.5] - 2019-08-08

### Added

- Added (DBMS specific) awareness of maximum table/database/column lengths into `IQuerySyntaxHelper`
- Create / Discover methods now validate the provided names before sending Sql to the DBMS (prevents attempts to create table names that are too long for the DBMS or entities containing periods or brackets)

### Fixed 
- Oracle no longer truncates strings in GetRuntimeName to 30

## [0.9.4] - 2019-07-29

### Fixed 
- Fixed bug creating Oracle tables from free text data containing extended ASCII / Unicode characters.

## [0.9.3] - 2019-07-19

### Added 

- Oracle support for Basic and Calendar table aggregates

### Fixed 

- DiscoveredTable.Rename now throws NotSupportedException for Views and TableValuedFunctions

## [0.9.2] - 2019-07-04

### Added 

- Oracle DiscoverTables now supports return view option
- MySql DiscoverTables now supports return view option

### Removed

- FAnsi.csproj no longer depends on System.Data.SqlClient (dependency moved to FAnsi.Implementations.MicrosoftSQL)

### Fixed 

- Fixed Oracle rename implementation
- Fixed DiscoverTables not correctly setting TableType for Views
- Fixed Drop table to work correctly with Views
- Exists now works correctly for Views (previously it would return true if there was no view but a table with the same name)

[Unreleased]: https://github.com/HicServices/FAnsiSql/compare/0.10.1...develop
[0.10.1]: https://github.com/HicServices/FAnsiSql/compare/0.10.0...0.10.1
[0.10.0]: https://github.com/HicServices/FAnsiSql/compare/0.9.8...0.10.0
[0.9.8]: https://github.com/HicServices/FAnsiSql/compare/0.9.7...0.9.8
[0.9.7]: https://github.com/HicServices/FAnsiSql/compare/0.9.6...0.9.7
[0.9.6]: https://github.com/HicServices/FAnsiSql/compare/0.9.5...0.9.6
[0.9.5]: https://github.com/HicServices/FAnsiSql/compare/0.9.4...0.9.5
[0.9.4]: https://github.com/HicServices/FAnsiSql/compare/0.9.3...0.9.4
[0.9.3]: https://github.com/HicServices/FAnsiSql/compare/0.9.2...0.9.3
[0.9.2]: https://github.com/HicServices/FAnsiSql/compare/v0.9.1.10...0.9.2
