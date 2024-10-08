﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery.Constraints;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using TypeGuesser;

namespace FAnsi.Discovery;

/// <summary>
/// Cross database type reference to a Table (or view) in a Database.  Use TableType to determine whether it is a view or a table.  Allows you to check
/// existence, drop, add columns, get row counts etc.
/// </summary>
public class DiscoveredTable : IHasFullyQualifiedNameToo, IMightNotExist, IHasQuerySyntaxHelper, IEquatable<DiscoveredTable>
{
    protected string TableName;

    /// <summary>
    /// Helper for generating queries compatible with the DBMS the table exists in (e.g. TOP X, column qualifiers, what the parameter symbol is etc).
    /// </summary>
    protected readonly IQuerySyntaxHelper QuerySyntaxHelper;

    /// <summary>
    /// The database on which the table exists
    /// </summary>
    public readonly DiscoveredDatabase Database;

    /// <summary>
    /// Stateless helper class with DBMS specific implementation of the logic required by <see cref="DiscoveredTable"/>.
    /// </summary>
    public readonly IDiscoveredTableHelper Helper;

    /// <summary>
    /// <para>Schema of the <see cref="Database"/> the table exists in (or null).  This is NOT the database e.g. in [MyDb].[dbo].[MyTable] the schema is "dbo".</para>
    /// 
    /// <para>Null if not supported by the DBMS (e.g. MySql)</para>
    /// </summary>
    public readonly string? Schema;

    /// <summary>
    /// Whether the table referenced is a normal table, view or table valued function (see derived class <see cref="DiscoveredTableValuedFunction"/>)
    /// </summary>
    public readonly TableType TableType;

    /// <summary>
    /// Internal API constructor intended for Implementation classes, instead use <see cref="DiscoveredDatabase.ExpectTable"/> instead.
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="querySyntaxHelper"></param>
    /// <param name="schema"></param>
    /// <param name="tableType"></param>
    public DiscoveredTable(DiscoveredDatabase database, string table, IQuerySyntaxHelper querySyntaxHelper, string? schema = null, TableType tableType = TableType.Table)
    {
        TableName = table;
        Helper = database.Helper.GetTableHelper();
        Database = database;
        Schema = schema;
        TableType = tableType;

        QuerySyntaxHelper = querySyntaxHelper;

        QuerySyntaxHelper.ValidateTableName(TableName);
    }

    /// <summary>
    /// <para>Checks that the <see cref="Database"/> exists then lists the tables in the database to confirm this table exists on the server</para>
    /// </summary>
    /// <param name="transaction">Optional - if set the connection to list tables will be sent on the connection on which the current
    /// <paramref name="transaction"/> is open</param>
    /// <returns></returns>
    public virtual bool Exists(IManagedTransaction? transaction = null)
    {
        if (!Database.Exists())
            return false;

        return Database.DiscoverTables(TableType == TableType.View, transaction)
            .Any(t => t.GetRuntimeName().Equals(GetRuntimeName(), StringComparison.InvariantCultureIgnoreCase));
    }

    /// <summary>
    /// Returns the unqualified name of the table e.g. "MyTable"
    /// </summary>
    /// <returns></returns>
    public virtual string GetRuntimeName() => QuerySyntaxHelper.GetRuntimeName(TableName);

    /// <summary>
    /// Returns the fully qualified (including schema if appropriate) name of the table e.g. [MyDb].dbo.[MyTable] or `MyDb`.`MyTable`
    /// </summary>
    /// <returns></returns>
    public virtual string GetFullyQualifiedName() => QuerySyntaxHelper.EnsureFullyQualified(Database.GetRuntimeName(), Schema, GetRuntimeName());

    /// <summary>
    /// Returns the wrapped e.g. "[MyTbl]" name of the table including escaping e.g. if you wanted to name a table "][nquisitor" (which would return "[]][nquisitor]").  Use <see cref="GetFullyQualifiedName()"/> to return the full name including table/database/schema.
    /// </summary>
    /// <returns></returns>
    public string GetWrappedName() => QuerySyntaxHelper.EnsureWrapped(GetRuntimeName());

    /// <summary>
    /// Connects to the server and returns a list of columns found in the table as <see cref="DiscoveredColumn"/>.
    /// </summary>
    /// <param name="managedTransaction">Optional - if set the connection to list tables will be sent on the connection on which the current
    /// <paramref name="managedTransaction"/> is open</param>
    /// <returns></returns>
    public DiscoveredColumn[] DiscoverColumns(IManagedTransaction? managedTransaction = null)
    {
        using var connection = Database.Server.GetManagedConnection(managedTransaction);
        return Helper.DiscoverColumns(this, connection, Database.GetRuntimeName()).ToArray();
    }

    /// <summary>
    /// Returns the table name
    /// </summary>
    /// <returns></returns>
    public override string ToString() => TableName;

    /// <summary>
    /// Gets helper for generating queries compatible with the DBMS the table exists in (e.g. TOP X, column qualifiers, what the parameter symbol is etc).
    /// </summary>
    /// <returns></returns>
    public IQuerySyntaxHelper GetQuerySyntaxHelper() => QuerySyntaxHelper;

    /// <summary>
    /// Returns from <see cref="DiscoverColumns"/> the <paramref name="specificColumnName"/> on the server.  This is not not case sensitive.  Requires
    /// connecting to the database.
    /// </summary>
    /// <param name="specificColumnName">The column you want to find</param>
    /// <param name="transaction">Optional - if set the connection to list tables will be sent on the connection on which the current
    /// <paramref name="transaction"/> is open</param>
    /// <returns></returns>
    public DiscoveredColumn DiscoverColumn(string specificColumnName, IManagedTransaction? transaction = null)
    {
        try
        {
            return DiscoverColumns(transaction).Single(c => c.GetRuntimeName().Equals(QuerySyntaxHelper.GetRuntimeName(specificColumnName), StringComparison.InvariantCultureIgnoreCase));
        }
        catch (InvalidOperationException e)
        {
            throw new ColumnMappingException(string.Format(
                FAnsiStrings.DiscoveredTable_DiscoverColumn_DiscoverColumn_failed__could_not_find_column_called___0___in_table___1__, specificColumnName,
                TableName), e);
        }
    }

    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetTopXSql"]'/>
    public string GetTopXSql(int topX) => Helper.GetTopXSqlForTable(this, topX);


    /// <summary>
    /// Returns up to 2,147,483,647 records from the table as a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="topX">The maximum number of records to return from the table</param>
    /// <param name="enforceTypesAndNullness">True to set <see cref="DataColumn"/> constraints on the <see cref="DataTable"/> returned e.g. AllowDBNull based on the table
    /// schema of the <see cref="DiscoveredTable"/></param>
    /// <param name="transaction">Optional - if set the connection to fetch the data will be sent on the connection on which the current <paramref name="transaction"/> is open</param>
    /// <returns></returns>
    public DataTable GetDataTable(int topX = int.MaxValue, bool enforceTypesAndNullness = true, IManagedTransaction? transaction = null) => GetDataTable(new DatabaseOperationArgs { TransactionIfAny = transaction }, topX, enforceTypesAndNullness);

    public DataTable GetDataTable(DatabaseOperationArgs args, int topX = int.MaxValue, bool enforceTypesAndNullness = true)
    {
        var dt = new DataTable();

        if (enforceTypesAndNullness)
            foreach (var c in DiscoverColumns(args.TransactionIfAny))
            {
                var col = dt.Columns.Add(c.GetRuntimeName());
                col.AllowDBNull = c.AllowNulls;
                col.DataType = c.DataType.GetCSharpDataType();
            }

        Helper.FillDataTableWithTopX(args, this, topX, dt);

        return dt;
    }

    /// <summary>
    /// Drops (deletes) the table from the database.  This is irreversible unless you have a database backup.
    /// </summary>
    public virtual void Drop()
    {
        using var connection = Database.Server.GetManagedConnection();
        Helper.DropTable(connection.Connection, this);
    }

    public int GetRowCount(IManagedTransaction? transaction = null) => GetRowCount(new DatabaseOperationArgs { TransactionIfAny = transaction });

    /// <summary>
    /// Returns the estimated number of rows in the table.  This may use a short cut e.g. consulting sys.partitions in Sql
    /// Server (https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-partitions-transact-sql?view=sql-server-2017)
    /// </summary>
    /// <param name="args">Options for the operation e.g timeout, using existing connection etc</param>
    /// <returns></returns>
    public int GetRowCount(DatabaseOperationArgs args) => Helper.GetRowCount(args, this);

    /// <summary>
    /// Returns true if there are no rows in the table
    /// </summary>
    /// <param name="transaction">Optional - if set the query will be sent on the connection on which the current <paramref name="transaction"/> is open</param>
    /// <returns></returns>
    public bool IsEmpty(IManagedTransaction? transaction = null) => IsEmpty(new DatabaseOperationArgs { TransactionIfAny = transaction });

    /// <summary>
    /// Returns true if there are no rows in the table
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    public bool IsEmpty(DatabaseOperationArgs args) => Helper.IsEmpty(args, this);

    /// <summary>
    /// Creates and runs an ALTER TABLE SQL statement that adds a new column to the table
    /// </summary>
    /// <param name="name">The unqualified name for the new column e.g. "MyCol2"</param>
    /// <param name="type">The data type for the new column</param>
    /// <param name="allowNulls">True to allow null</param>
    /// <param name="timeoutInSeconds">The length of time to wait in seconds before giving up (See <see cref="DbCommand.CommandTimeout"/>)</param>
    public void AddColumn(string name, DatabaseTypeRequest type, bool allowNulls, int timeoutInSeconds)
    {
        AddColumn(name, type, allowNulls, new DatabaseOperationArgs { TimeoutInSeconds = timeoutInSeconds });
    }

    /// <summary>
    /// Creates and runs an ALTER TABLE SQL statement that adds a new column to the table
    /// </summary>
    /// <param name="name">The unqualified name for the new column e.g. "MyCol2"</param>
    /// <param name="type">The data type for the new column</param>
    /// <param name="allowNulls">True to allow null</param>
    /// <param name="args"></param>
    public void AddColumn(string name, DatabaseTypeRequest type, bool allowNulls, DatabaseOperationArgs args)
    {
        AddColumn(name, Database.Server.GetQuerySyntaxHelper().TypeTranslater.GetSQLDBTypeForCSharpType(type), allowNulls, args);
    }

    /// <summary>
    /// Creates and runs an ALTER TABLE SQL statement that adds a new column to the table
    /// </summary>
    /// <param name="name">The unqualified name for the new column e.g. "MyCol2"</param>
    /// <param name="databaseType">The proprietary SQL data type for the new column</param>
    /// <param name="allowNulls">True to allow null</param>
    /// <param name="timeoutInSeconds">The length of time to wait in seconds before giving up (See <see cref="DbCommand.CommandTimeout"/>)</param>
    public void AddColumn(string name, string databaseType, bool allowNulls, int timeoutInSeconds)
    {
        AddColumn(name, databaseType, allowNulls, new DatabaseOperationArgs { TimeoutInSeconds = timeoutInSeconds });
    }

    public void AddColumn(string name, string databaseType, bool allowNulls, DatabaseOperationArgs args)
    {
        Helper.AddColumn(args, this, name, databaseType, allowNulls);
    }

    /// <summary>
    /// Creates and runs an ALTER TABLE SQL statement to drop the given column from the table
    /// </summary>
    /// <param name="column">The column to drop</param>
    public void DropColumn(DiscoveredColumn column)
    {
        using var connection = Database.Server.GetManagedConnection();
        Helper.DropColumn(connection.Connection, column);
    }

    /// <summary>
    /// Creates a new object for bulk inserting records into the table.  You should use a using block since <see cref="IBulkCopy"/> is <see cref="IDisposable"/>.
    /// Depending on implementation, records may not be committed to the server until the <see cref="IBulkCopy"/> is disposed.
    /// </summary>
    /// <param name="transaction">Optional - records inserted should form part of the supplied ongoing transaction</param>
    /// <returns></returns>
    public IBulkCopy BeginBulkInsert(IManagedTransaction? transaction = null) => BeginBulkInsert(CultureInfo.CurrentCulture, transaction);

    /// <summary>
    /// Creates a new object for bulk inserting records into the table.  You should use a using block since <see cref="IBulkCopy"/> is <see cref="IDisposable"/>.
    /// Depending on implementation, records may not be committed to the server until the <see cref="IBulkCopy"/> is disposed.
    /// </summary>
    /// <param name="culture"></param>
    /// <param name="transaction">Optional - records inserted should form part of the supplied ongoing transaction</param>
    /// <returns></returns>
    public IBulkCopy BeginBulkInsert(CultureInfo culture, IManagedTransaction? transaction = null)
    {
        Database.Server.EnableAsync();
        var connection = Database.Server.GetManagedConnection(transaction);
        return Helper.BeginBulkInsert(this, connection, culture);
    }

    /// <summary>
    /// Creates and runs a TRUNCATE TABLE SQL statement to delete all rows from the table.  Depending on DBMS and table constraints this might fail (e.g. if there are
    /// foreign key constraints on the table).
    /// </summary>
    public void Truncate()
    {
        Helper.TruncateTable(this);
    }

    /// <summary>
    /// Deletes all EXACT duplicate rows from the table leaving only unique records.  This is method may not be transaction/threadsafe
    /// </summary>
    /// <param name="timeoutInSeconds">The length of time to allow for the command to complete (See <see cref="DbCommand.CommandTimeout"/>)</param>
    public void MakeDistinct(int timeoutInSeconds = 30)
    {
        MakeDistinct(new DatabaseOperationArgs { TimeoutInSeconds = timeoutInSeconds });
    }

    /// <summary>
    /// Deletes all EXACT duplicate rows from the table leaving only unique records.  This is method may not be transaction/threadsafe
    /// </summary>
    /// <param name="args">Options for timeout, transaction etc</param>
    public void MakeDistinct(DatabaseOperationArgs args)
    {
        Helper.MakeDistinct(args, this);
    }


    /// <summary>
    /// <para>Scripts the table columns, optionally adjusting for nullability / identity etc.  Optionally translates the SQL to run and create a table in a different
    /// database / database language / table name</para>
    /// 
    /// <para>Does not include foreign key constraints, dependant tables, CHECK constraints etc</para>
    /// </summary>
    /// <param name="dropPrimaryKeys">True if the resulting script should exclude any primary keys</param>
    /// <param name="dropNullability">True if the resulting script should always allow nulls into columns</param>
    /// <param name="convertIdentityToInt">True if the resulting script should replace identity columns with int in the generated SQL</param>
    /// <param name="toCreateTable">Optional, If provided the SQL generated will be adjusted to create the alternate table instead (which could include going cross server type e.g. MySql to Sql Server)
    /// <para>When using this parameter the table must not exist yet, use destinationDiscoveredDatabase.ExpectTable("MyYetToExistTable")</para></param>
    /// <returns></returns>
    public string ScriptTableCreation(bool dropPrimaryKeys, bool dropNullability, bool convertIdentityToInt, DiscoveredTable? toCreateTable = null) => Helper.ScriptTableCreation(this, dropPrimaryKeys, dropNullability, convertIdentityToInt, toCreateTable);

    /// <summary>
    /// Issues a database command to rename the table on the database server.
    /// </summary>
    /// <param name="newName"></param>
    public void Rename(string newName)
    {
        using var connection = Database.Server.GetManagedConnection();
        Helper.RenameTable(this, newName, connection);
        TableName = newName;
    }

    /// <summary>
    /// Creates a primary key on the table if none exists yet
    /// </summary>
    /// <param name="discoverColumns">Columns that should become part of the primary key</param>
    public void CreatePrimaryKey(params DiscoveredColumn[] discoverColumns)
    {
        CreatePrimaryKey(new DatabaseOperationArgs(), discoverColumns);
    }

    /// <summary>
    /// Creates a primary key on the table if none exists yet
    /// </summary>
    /// <param name="timeoutInSeconds">The number of seconds to wait for the operation to complete</param>
    /// <param name="discoverColumns">Columns that should become part of the primary key</param>
    public void CreatePrimaryKey(int timeoutInSeconds, params DiscoveredColumn[] discoverColumns)
    {
        CreatePrimaryKey(new DatabaseOperationArgs { TimeoutInSeconds = timeoutInSeconds }, discoverColumns);
    }

    /// <summary>
    /// Creates a primary key on the table if none exists yet
    /// </summary>
    /// <param name="transaction">Optional ongoing transaction to use (leave null if not needed)</param>
    /// <param name="token">Token for cancelling the command mid execution (leave null if not needed)</param>
    /// <param name="timeoutInSeconds">The number of seconds to wait for the operation to complete</param>
    /// <param name="discoverColumns">Columns that should become part of the primary key</param>
    public void CreatePrimaryKey(IManagedTransaction? transaction, CancellationToken token, int timeoutInSeconds, params DiscoveredColumn[] discoverColumns)
    {
        Helper.CreatePrimaryKey(new DatabaseOperationArgs
        {
            TransactionIfAny = transaction,
            CancellationToken = token,
            TimeoutInSeconds = timeoutInSeconds
        }, this, discoverColumns);
    }

    /// <summary>
    /// Creates an index on the table
    /// </summary>
    /// <param name="indexName"></param>
    /// <param name="discoverColumns"></param>
    /// <param name="isUnique"></param>
    public void CreateIndex(string indexName, DiscoveredColumn[] discoverColumns, bool isUnique = false)
    {
        CreateIndex(new DatabaseOperationArgs(), indexName, discoverColumns, isUnique);
    }

    /// <summary>
    /// Creates an index on the table
    /// </summary>
    /// <param name="args"></param>
    /// <param name="indexName"></param>
    /// <param name="discoverColumns"></param>
    /// <param name="isUnique"></param>
    public void CreateIndex(DatabaseOperationArgs args, string indexName, DiscoveredColumn[] discoverColumns, bool isUnique = false)
    {
        Helper.CreateIndex(args, this, indexName, discoverColumns, isUnique);
    }

    /// <summary>
    /// Drops the specified index from the discovered table
    /// </summary>
    /// <param name="indexName"></param>
    public void DropIndex(string indexName)
    {
        DropIndex(new DatabaseOperationArgs(), indexName);
    }

    /// <summary>
    /// Drops the specified index from the discovered table
    /// </summary>
    /// <param name="args"></param>
    /// <param name="indexName"></param>
    public void DropIndex(DatabaseOperationArgs args, string indexName)
    {
        Helper.DropIndex(args, this, indexName);
    }

    public void CreatePrimaryKey(DatabaseOperationArgs args, params DiscoveredColumn[] discoverColumns)
    {
        Helper.CreatePrimaryKey(args, this, discoverColumns);
    }

    /// <summary>
    /// Inserts the values specified into the database table and returns the last autonum identity generated (or 0 if none present)
    /// </summary>
    /// <param name="toInsert"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    public int Insert(Dictionary<DiscoveredColumn, object> toInsert, IManagedTransaction? transaction = null) => Insert(toInsert, null, transaction);

    /// <summary>
    /// Inserts the values specified into the database table and returns the last autonum identity generated (or 0 if none present)
    /// </summary>
    /// <param name="toInsert"></param>
    /// <param name="culture"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    public int Insert(Dictionary<DiscoveredColumn, object> toInsert, CultureInfo? culture, IManagedTransaction? transaction = null)
    {
        var syntaxHelper = GetQuerySyntaxHelper();
        var server = Database.Server;

        var _parameterNames = syntaxHelper.GetParameterNamesFor(toInsert.Keys.ToArray(), static c => c.GetRuntimeName());

        using var connection = Database.Server.GetManagedConnection(transaction);
        var sql =
            $"INSERT INTO {GetFullyQualifiedName()}({string.Join(",", toInsert.Keys.Select(c => syntaxHelper.EnsureWrapped(c.GetRuntimeName())))}) VALUES ({string.Join(",", toInsert.Keys.Select(c => _parameterNames[c]))})";

        using var cmd = server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
        foreach (var p in toInsert
                     .Select(kvp => new { kvp, parameter = server.Helper.GetParameter(_parameterNames[kvp.Key]) })
                     .Select(t =>
                         GetQuerySyntaxHelper().GetParameter(t.parameter, t.kvp.Key, t.kvp.Value, culture)))
            cmd.Parameters.Add(p);

        return Helper.ExecuteInsertReturningIdentity(this, cmd, connection.ManagedTransaction);
    }

    /// <summary>
    /// Overload which will discover the columns by name for you.
    /// </summary>
    /// <param name="toInsert"></param>
    /// <param name="transaction">ongoing transaction this insert should be part of</param>
    /// <returns></returns>
    public int Insert(Dictionary<string, object> toInsert, IManagedTransaction? transaction = null) => Insert(toInsert, null, transaction);

    /// <summary>
    /// Overload which will discover the columns by name for you.
    /// </summary>
    /// <param name="toInsert"></param>
    /// <param name="culture"></param>
    /// <param name="transaction">ongoing transaction this insert should be part of</param>
    /// <returns></returns>
    public int Insert(Dictionary<string, object> toInsert, CultureInfo? culture, IManagedTransaction? transaction = null)
    {
        var cols = DiscoverColumns(transaction);

        var foundColumns = new Dictionary<DiscoveredColumn, object>();

        foreach (var k in toInsert.Keys)
        {
            var match =
                cols.SingleOrDefault(c => c.GetRuntimeName().Equals(k, StringComparison.InvariantCultureIgnoreCase)) ??
                throw new ColumnMappingException(string.Format(
                    FAnsiStrings
                        .DiscoveredTable_Insert_Insert_failed__could_not_find_column_called___0___in_table___1__, k,
                    TableName));

            foundColumns.Add(match, toInsert[k]);
        }

        return Insert(foundColumns, culture, transaction);
    }
    /// <summary>
    /// See <see cref="DiscoveredServerHelper.GetCommand"/>
    /// </summary>
    public DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) => Database.Server.Helper.GetCommand(s, con, transaction);

    /// <summary>
    /// Returns all foreign keys where this table is the parent table (i.e. the primary key table).
    /// </summary>
    /// <param name="transaction"></param>
    /// <returns></returns>
    public DiscoveredRelationship[] DiscoverRelationships(IManagedTransaction? transaction = null)
    {
        using var connection = Database.Server.GetManagedConnection(transaction);
        return Helper.DiscoverRelationships(this, connection.Connection, transaction).ToArray();
    }

    /// <summary>
    /// Based on table name, schema, database and TableType
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public bool Equals(DiscoveredTable? other)
    {
        if (other is null) return false;

        return
            string.Equals(TableName, other.TableName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(GetSchemaWithDefaultForNull(), other.GetSchemaWithDefaultForNull(), StringComparison.OrdinalIgnoreCase)
            && Equals(Database, other.Database) && TableType == other.TableType;
    }

    private string? GetSchemaWithDefaultForNull() =>
        //for "dbo, "" and null are all considered the same
        string.IsNullOrWhiteSpace(Schema) ? GetQuerySyntaxHelper().GetDefaultSchemaIfAny() : Schema;

    /// <summary>
    /// Based on table name, schema, database and TableType
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override bool Equals(object? obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;

        return Equals((DiscoveredTable)obj);
    }

    /// <summary>
    /// Based on table name, schema, database and TableType
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = StringComparer.OrdinalIgnoreCase.GetHashCode(GetSchemaWithDefaultForNull() ?? string.Empty);
            hashCode = (hashCode * 397) ^ (Database != null ? Database.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (int)TableType;
            return hashCode;
        }
    }

    public DiscoveredRelationship AddForeignKey(DiscoveredColumn foreignKey, DiscoveredColumn primaryKey, bool cascadeDeletes, string? constraintName = null, DatabaseOperationArgs? args = null) => AddForeignKey(new Dictionary<DiscoveredColumn, DiscoveredColumn> { { foreignKey, primaryKey } }, cascadeDeletes, constraintName, args);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="foreignKeyPairs">
    /// Key is the foreign key column (and the table the constraint will be put on).
    /// Value is the primary key table column (which the constraint reference points to)</param>
    /// <param name="cascadeDeletes"></param>
    /// <param name="constraintName">Specify an explicit name for the foreign key, leave null to pick one arbitrarily</param>
    /// <param name="args">Options for timeout, transaction etc</param>
    /// <returns></returns>
    public DiscoveredRelationship AddForeignKey(Dictionary<DiscoveredColumn, DiscoveredColumn> foreignKeyPairs,
        bool cascadeDeletes, string? constraintName = null, DatabaseOperationArgs? args = null) =>
        Helper.AddForeignKey(args ?? new DatabaseOperationArgs(), foreignKeyPairs, cascadeDeletes, constraintName);

}