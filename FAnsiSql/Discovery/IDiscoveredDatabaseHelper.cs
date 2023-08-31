using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.TableCreation;
using FAnsi.Naming;

namespace FAnsi.Discovery;

/// <summary>
/// Contains all the DatabaseType specific implementation logic required by DiscoveredDatabase.
/// </summary>
public interface IDiscoveredDatabaseHelper
{

    IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction transaction = null);
    IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, DbTransaction transaction = null);

    DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database);

    IDiscoveredTableHelper GetTableHelper();
    void DropDatabase(DiscoveredDatabase database);

    Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database);

    DiscoveredTable CreateTable(CreateTableArgs args);

    string GetCreateTableSql(DiscoveredDatabase database, string tableName, DatabaseColumnRequest[] columns, Dictionary<DatabaseColumnRequest, DiscoveredColumn> foreignKeyPairs, bool cascadeDelete,string schema = null);

    /// <summary>
    /// Generates foreign key creation SQL such that it can be slotted into either a CREATE TABLE statement OR a ALTER TABLE statement
    /// </summary>
    /// <param name="foreignTable">The foreign table on which to declare the constraint</param>
    /// <param name="syntaxHelper">The language to use e.g. for wrapping entity names</param>
    /// <param name="foreignKeyPairs">The columns to match up, key must be either <see cref="DiscoveredColumn"/> or <see cref="DatabaseColumnRequest"/>.
    ///
    /// Key is the foreign key column (and the table the constraint will be put on).
    /// Value is the primary key table column (which the constraint reference points to)
    /// </param>
    /// <param name="cascadeDelete">True to add the on delete cascade rules</param>
    /// <param name="constraintName">The name of the new constraint to create or null to use default</param>
    /// <returns></returns>
    string GetForeignKeyConstraintSql(string foreignTable, IQuerySyntaxHelper syntaxHelper,
        Dictionary<IHasRuntimeName, DiscoveredColumn> foreignKeyPairs, bool cascadeDelete, string constraintName = null);

    DirectoryInfo Detach(DiscoveredDatabase database);
    void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName);

    /// <summary>
    /// Gets a sensible name for a foreign key constraint between the two tables
    /// </summary>
    /// <param name="foreignTable"></param>
    /// <param name="primaryTable"></param>
    /// <returns></returns>
    string GetForeignKeyConstraintNameFor(DiscoveredTable foreignTable, DiscoveredTable primaryTable);

    /// <summary>
    /// Throws an <see cref="NotSupportedException"/> if the <paramref name="dt"/> contains <see cref="DataColumn"/> with
    /// the <see cref="DataColumn.DataType"/> of <see cref="System.Object"/>
    /// </summary>
    /// <param name="dt"></param>
    void ThrowIfObjectColumns(DataTable dt);

    /// <summary>
    /// Creates a new schema within the database if the DBMS supports it (Sql Server does, MySql doesn't) and it does not already exist.  Schema
    /// is a layer below server and database but above table it groups tables within a single database.
    /// 
    /// </summary>
    /// <param name="discoveredDatabase">Database to create schema in</param>
    /// <param name="name"></param>
    /// <returns></returns>
    void CreateSchema(DiscoveredDatabase discoveredDatabase, string name);
}