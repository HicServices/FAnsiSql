# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added

- Added (DBMS specific) awareness of maximum table/database/column lengths into `IQuerySyntaxHelper`

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

[Unreleased]: https://github.com/HicServices/FAnsiSql/compare/0.9.4...develop
[0.9.4]: https://github.com/HicServices/FAnsiSql/compare/0.9.3...0.9.4
[0.9.3]: https://github.com/HicServices/FAnsiSql/compare/0.9.2...0.9.3
[0.9.2]: https://github.com/HicServices/FAnsiSql/compare/v0.9.1.10...0.9.2
