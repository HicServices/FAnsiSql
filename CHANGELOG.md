# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.2.0] - 2023-03-04

- Target .Net 8.0
- Enable AOT compatibility, though upstream dependencies still have issues
- Nullable annotations enabled (some warnings remain)
- Bump HIC.TypeGuesser from 1.1.0 to 1.2.3
- Bump Microsoft.Data.SqlClient from 5.1.1 to 5.2.0
- Bump MySqlConnector from 2.2.6 to 2.3.5
- Bump Npgsql from 7.0.4 to 8.0.2
- Bump Oracle.ManagedDataAccess.Core from 3.21.100 to 3.21.130

## [3.1.1] - 2023-09-01

- Bugfix: MySQL text was erroneously capped at 64k (TEXT) instead of LONGTEXT (4GiB)
- Adjust timeout handling, use 10 seconds not 3 for server live tests since our Azure VM is slow
- Bump MySqlConnector from 2.2.6 to 2.2.7
- Bump Oracle.ManagedDataAccess.Core from 3.21.100 to 3.21.110

## [3.1.0] - 2023-05-15

- Now targeting .Net 6
- Single assembly build, more single-file and AOT friendly
- Oracle CI tests implemented, multiple bug fixes (stop mistaking Oracle LONG blob type for an integer value, wrap column names correctly, fix naming limits)
- Bump HIC.TypeGuesser from 1.0.3 to 1.1.0
- Bump Microsoft.Data.SqlClient from 5.0.1 to 5.1.1
- Bump MySqlConnector from 2.1.13 to 2.2.6
- Bump Npgsql from 6.0.7 to 7.0.4
- Bump Oracle.ManagedDataAccess.Core from 2.19.101 to 3.21.100
- Eliminate System.ComponentModel.Composition dependency

## [3.0.1] - 2022-10-28

### Fixed

- Fixed bug where passing empty string to `EnsureWrapped` would return wrapped empty string e.g. `[]`

## [3.0.0] - 2022-08-29

### Fixed

- Fixed bug with Aggregate graph in some specific versions of MySql

### Changed

- Switched to targeting net standard 2.1 (previously of 2.0)

## [2.0.5] - 2022-08-23

### Fixed

- Fixed returning length estimate of -1 for `ntext` datatype
- Fixed not sorting by date in Sql Server Calendar table with Pivot aggregate

## [2.0.4] - 2022-04-21

### Fixed

- Fixed parameter detection to pick up names immediately after commas

## [2.0.3] - 2022-02-22

### Changed

- `IntegratedSecurity` is now disabled when creating `SqlConnectionStringBuilder` instances where `Authentication` keyword is specified (Azure compatibility)
- Made SqlServer `SET SINGLE_USER [...]` SQL optional when issuing Drop Database.  Configure with `MicrosoftSQLDatabaseHelper.SetSingleUserWhenDroppingDatabases`.
- When sending `SET SINGLE_USER [...]` SQL during `DROP DATABASE` on an SqlServer fails, try the drop again without the SINGLE_USER statement.

### Dependencies

- Bump MySqlConnector from 2.1.5 to 2.1.6

## [2.0.2] - 2022-02-03

### Changed

- Bump Microsoft.Data.SqlClient from 3.0.1 to 4.1.0
- Bump Npgsql from 5.0.7 to 6.0.3
- Bump MySqlConnector from 1.3.11 to 2.1.5

## [2.0.1] - 2021-07-27

### Changed

- Upgraded Sql Server library from `System.Data.SqlClient` to `Microsoft.Data.SqlClient`
- Bump MySqlConnector from 1.3.9 to 1.3.11
- Bump Npgsql from 5.0.5 to 5.0.7

### Added

- Added `CreateDatabaseTimeoutInSeconds` static property to `DiscoveredServerHelper`


## [1.0.7] - 2021-05-18

### Added

- String parse errors in bulk insert now include the column name

### Changed

- Bump Oracle.ManagedDataAccess.Core from 2.19.100 to 2.19.101
- Bump Npgsql from 5.0.1.1 to 5.0.5
- Bump MySqlConnector from 1.2.1 to 1.3.9

## [1.0.6] - 2020-09-16

### Added

- Support for ExplicitDateFormats in CreateTable and BulkInsert

## [1.0.5] - 2020-08-13

### Added

- Updated IQuerySyntaxHelper to expose properties (OpenQualifier, CloseQualifier, DatabaseTableSeparator, IllegalNameChars)

### Fixed

- Fixed bug in repeated calls to GetRuntimeName and EnsureWrapped when a column/table name had escaped qualifiers (e.g. `[Hey]]There]`)
 
## [1.0.4] - 2020-08-10

### Fixed

- Fixed bug in CreateSchema (Sql Server) where a schema with the same name already exists.  This bug was introduced in 1.0.3 (only affects repeated calls).

## [1.0.3] - 2020-08-06

### Changed

- Updated MySqlConnector to 1.0.0

### Added

- Added `GetWrappedName` method for columns/tables for when a full expression is not allowed but wrapping is still needed. 

## [1.0.2] - 2020-07-07

### Fixed

- Fixed Nuget package dependencies

## [1.0.1] - 2020-07-07

### Fixed

- Updated dependencies, fixing issue in uploading string typed big integers e.g. `"9223372036854775807"`
- Fixed table creation and column discovery when column/table names containing backticks and/or single quotes

## [0.11.0] - 2020-02-27

### Changed

- Changed client library from MySql.Data to [MySqlConnector](https://github.com/mysql-net/MySqlConnector)
  - If you have any connection strings with `Ssl-Mode` change it to `SSLMode` (i.e. remove the hyphen)
  - Update your package references (if any)

## [0.10.13] - 2019-11-25

### Fixed

- Fixed `MakeDistinct` for Sql Server not wrapping column names (Only causes problems when using columns with spaces / reserved words)

## [0.10.12] - 2019-11-19

### Fixed

- Fixed bug where `GetFullyQualifiedName` in MySql would not wrap the column name in quotes

## [0.10.11] - 2019-11-18

### Fixed

- Fixed bug reading `text` data types out of Postgres databases (would be read as invalid type `varchar(max)`)

## [0.10.10] - 2019-11-07

### Added

- Added `IQuerySyntaxHelper.SupportsEmbeddedParameters()` which returns whether or not the DBMS supports embedded SQL only parameters (e.g. `DECLARE @bob varchar(10)`).  In order to be qualify the DBMS must:
  - have a pure SQL only declaration format (i.e. not injected from outside)
  - support variable values canging during the query
  - not require mutilating the entire SQL query (e.g. with BEGIN / END ) blocks and indentation
  - not affect normal behaviour's such as SELECT returning result set from query

### Fixed

- AddColumn now works properly with dodgy column names (e.g. `"My Fun New Column[Lol]"`)
- MySql `GetConnectionStringBuilder` method no longer swallows platform exceptions around trusted security

## [0.10.9] - 2019-11-04

### Fixed

- Fixed Postgres escaped names (e.g `"MyCol"`) now properly strip `"` when calling `GetRuntimeName`


## [0.10.8] - 2019-11-04


### Added

- Support for Postgres DBMS

### Fixed

- Fixed Oracle `long` mapping (previously mapped to "bigint" now maps to "long")


## [0.10.7] - 2019-09-20

### Added

- Task cancellation support for various long running operations (e.g. CreatePrimaryKey)
- Added Schema creation method to `DiscoveredDatabase`

### Changed

- Sql Server `GetRowCount` no longer uses `sys.partitions` which is unreliable (now just runs `select count(*)` like other implementations)

### Fixed

- Fixed connection leaking when using `BeginNewTransactedConnection` in a `using` block without calling either `CommitAndCloseConnection` or `AbandonAndCloseConnection`

## [0.10.6] - 2019-09-16

### Fixed

- Fixed bug when calling SetDoNotReType multiple times on the same DataColumn

## [0.10.5] - 2019-09-16

### Added

- Added ability to control T/F Y/N interpretation (as either bit or varchar column)

### Changed

- Updated TypeGuesser to 0.0.4

## [0.10.4] - 2019-09-11

### Added
- Added extension method `DataColumn.DoNotReType()` which suppresses Type changes on a column (e.g. during CreateTable calls)

### Fixed
- Fixed bug where culture was set after evaluating DataColumn contents during CreateTable
- Trying to create / upload DataTables which have columns of type System.Object now results in NotSupportedException (previously caused unstable behaviour depending on what object Types were put in table)

## [0.10.3] - 2019-09-10

### Changed

- Updated TypeGuesser to 0.0.3 (improves performance and trims trailing zeros from decimals).

## [0.10.2] - 2019-09-05

### Added

- Foreign Key constraints can now be added to tables using new method `DiscoveredTable.AddForeignKey`

### Fixed

- Fixed bug in MySql where `DiscoveredTable.DiscoverRelationships(...)` could throw an ArgumentException ("same key has already been added [...]") in some circumstances
 
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

[Unreleased]: https://github.com/HicServices/FAnsiSql/compare/v3.2.0...develop
[3.2.0]: https://github.com/HicServices/FAnsiSql/compare/v3.1.1...v3.2.0
[3.1.1]: https://github.com/HicServices/FAnsiSql/compare/v3.1.0...v3.1.1
[3.1.0]: https://github.com/HicServices/FAnsiSql/compare/3.0.1...v3.1.0
[3.0.1]: https://github.com/HicServices/FAnsiSql/compare/3.0.0...3.0.1
[3.0.0]: https://github.com/HicServices/FAnsiSql/compare/2.0.5...3.0.0
[2.0.5]: https://github.com/HicServices/FAnsiSql/compare/2.0.4...2.0.5
[2.0.4]: https://github.com/HicServices/FAnsiSql/compare/2.0.3...2.0.4
[2.0.3]: https://github.com/HicServices/FAnsiSql/compare/2.0.2...2.0.3
[2.0.2]: https://github.com/HicServices/FAnsiSql/compare/2.0.1...2.0.2
[2.0.1]: https://github.com/HicServices/FAnsiSql/compare/1.0.7...2.0.1
[1.0.7]: https://github.com/HicServices/FAnsiSql/compare/1.0.6...1.0.7
[1.0.6]: https://github.com/HicServices/FAnsiSql/compare/1.0.5...1.0.6
[1.0.5]: https://github.com/HicServices/FAnsiSql/compare/1.0.4...1.0.5
[1.0.4]: https://github.com/HicServices/FAnsiSql/compare/1.0.3...1.0.4
[1.0.3]: https://github.com/HicServices/FAnsiSql/compare/1.0.2...1.0.3
[1.0.2]: https://github.com/HicServices/FAnsiSql/compare/1.0.1...1.0.2
[1.0.1]: https://github.com/HicServices/FAnsiSql/compare/0.11.0...1.0.1
[0.11.0]: https://github.com/HicServices/FAnsiSql/compare/0.10.13...0.11.0
[0.10.13]: https://github.com/HicServices/FAnsiSql/compare/0.10.12...0.10.13
[0.10.12]: https://github.com/HicServices/FAnsiSql/compare/0.10.11...0.10.12
[0.10.11]: https://github.com/HicServices/FAnsiSql/compare/0.10.10...0.10.11
[0.10.10]: https://github.com/HicServices/FAnsiSql/compare/0.10.9...0.10.10
[0.10.9]: https://github.com/HicServices/FAnsiSql/compare/0.10.8...0.10.9
[0.10.8]: https://github.com/HicServices/FAnsiSql/compare/0.10.7...0.10.8
[0.10.7]: https://github.com/HicServices/FAnsiSql/compare/0.10.6...0.10.7
[0.10.6]: https://github.com/HicServices/FAnsiSql/compare/0.10.5...0.10.6
[0.10.5]: https://github.com/HicServices/FAnsiSql/compare/0.10.4...0.10.5
[0.10.4]: https://github.com/HicServices/FAnsiSql/compare/0.10.3...0.10.4
[0.10.3]: https://github.com/HicServices/FAnsiSql/compare/0.10.2...0.10.3
[0.10.2]: https://github.com/HicServices/FAnsiSql/compare/0.10.1...0.10.2
[0.10.1]: https://github.com/HicServices/FAnsiSql/compare/0.10.0...0.10.1
[0.10.0]: https://github.com/HicServices/FAnsiSql/compare/0.9.8...0.10.0
[0.9.8]: https://github.com/HicServices/FAnsiSql/compare/0.9.7...0.9.8
[0.9.7]: https://github.com/HicServices/FAnsiSql/compare/0.9.6...0.9.7
[0.9.6]: https://github.com/HicServices/FAnsiSql/compare/0.9.5...0.9.6
[0.9.5]: https://github.com/HicServices/FAnsiSql/compare/0.9.4...0.9.5
[0.9.4]: https://github.com/HicServices/FAnsiSql/compare/0.9.3...0.9.4
[0.9.3]: https://github.com/HicServices/FAnsiSql/compare/0.9.2...0.9.3
[0.9.2]: https://github.com/HicServices/FAnsiSql/compare/v0.9.1.10...0.9.2
