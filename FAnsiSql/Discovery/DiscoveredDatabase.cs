using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.TableCreation;
using FAnsi.Naming;
using TypeGuesser;

namespace FAnsi.Discovery;

/// <summary>
/// Cross database type reference to a specific database on a database server.  Allows you to create tables, drop check existance etc.
/// </summary>
public sealed class DiscoveredDatabase : IHasRuntimeName, IMightNotExist
{
    private readonly string _database;
    private readonly IQuerySyntaxHelper _querySyntaxHelper;

    /// <summary>
    /// The server on which the database exists
    /// </summary>
    public DiscoveredServer Server { get; }

    /// <summary>
    /// Stateless helper class with DBMS specific implementation of the logic required by <see cref="DiscoveredDatabase"/>.
    /// </summary>
    public IDiscoveredDatabaseHelper Helper { get; }

    /// <summary>
    /// API constructor, instead use <see cref="DiscoveredServer.ExpectDatabase"/> instead.
    /// </summary>
    /// <param name="server"></param>
    /// <param name="database"></param>
    /// <param name="querySyntaxHelper"></param>
    internal DiscoveredDatabase(DiscoveredServer server, string database, IQuerySyntaxHelper querySyntaxHelper)
    {
        Server = server;
        _database = database;
        _querySyntaxHelper = querySyntaxHelper;
        Helper = server.Helper.GetDatabaseHelper();

        _querySyntaxHelper.ValidateDatabaseName(database);
    }

    /// <summary>
    /// Connects to the server and returns a list of tables/views found as <see cref="DiscoveredTable"/>.  If you know your table exists and you only want to find one you
    /// can use <see cref="ExpectTable"/> instead.
    /// </summary>
    /// <param name="includeViews">true to also return views (See <see cref="DiscoveredTable.TableType"/>)</param>
    /// <param name="transaction">Optional - if provided the database query will be sent using the connection/transaction provided</param>
    /// <returns></returns>
    public DiscoveredTable[] DiscoverTables(bool includeViews, IManagedTransaction transaction = null)
    {
        using var managedConnection = Server.GetManagedConnection(transaction);
        return
            Helper.ListTables(this, _querySyntaxHelper, managedConnection.Connection, GetRuntimeName(),
                includeViews, managedConnection.Transaction).ToArray();
    }

    /// <summary>
    /// Connects to the server and returns a list of table valued functions found as <see cref="DiscoveredTableValuedFunction"/>.  If you know your function exists and you
    /// only want to find one you can use <see cref="ExpectTableValuedFunction"/> instead.
    /// </summary>
    /// <param name="transaction">Optional - if provided the database query will be sent using the connection/transaction provided</param>
    /// <returns></returns>
    public IEnumerable<DiscoveredTableValuedFunction> DiscoverTableValuedFunctions(IManagedTransaction transaction = null)
    {
        using var managedConnection = Server.GetManagedConnection(transaction);
        return
            Helper.ListTableValuedFunctions(this, _querySyntaxHelper, managedConnection.Connection,
                GetRuntimeName(), managedConnection.Transaction).ToArray();
    }

    /// <summary>
    /// Returns the name of the database without any qualifiers
    /// </summary>
    /// <returns></returns>
    public string GetRuntimeName()
    {
        return _querySyntaxHelper.GetRuntimeName(_database);
    }

    /// <summary>
    /// Returns the wrapped e.g. "[MyDatabase]" name of the database including escaping e.g. if you wanted to name a database "][nquisitor" (which would return "[]][nquisitor]").
    /// </summary>
    /// <returns></returns>
    public string GetWrappedName()
    {
        return _querySyntaxHelper.EnsureWrapped(GetRuntimeName());
    }

    /// <summary>
    /// <para>Creates an expectation (See <see cref="IMightNotExist"/>) that there is a table with the given name in the database.
    /// This method does not query the database or confirm it exists.</para>
    /// 
    /// <para>See also <see cref="DiscoveredTable.Exists"/></para>
    /// </summary>
    /// <param name="tableName">The runtime name (not qualified) of the table / view / function you are looking for</param>
    /// <param name="schema">Optional - The schema (if supported by DBMS) it exists in.  This is NOT the database e.g. in [MyDb].[dbo].[MyTable] the schema is "dbo".
    /// If in doubt leave blank</param>
    /// <param name="tableType">What you are looking for (normal table, view or table valued function)</param>
    /// <returns></returns>
    public DiscoveredTable ExpectTable(string tableName, string schema = null, TableType tableType = TableType.Table)
    {
        if (tableType == TableType.TableValuedFunction)
            return ExpectTableValuedFunction(tableName, schema);

        return new DiscoveredTable(this, tableName, _querySyntaxHelper, schema, tableType);
    }

    /// <inheritdoc cref="ExpectTable"/>
    public DiscoveredTableValuedFunction ExpectTableValuedFunction(string tableName,string schema = null)
    {
        return new DiscoveredTableValuedFunction(this, tableName, _querySyntaxHelper, schema);
    }

    /// <summary>
    /// Connects to the database and returns a list of stored proceedures found as <see cref="DiscoveredStoredprocedure"/> objects
    /// </summary>
    /// <returns></returns>
    public DiscoveredStoredprocedure[] DiscoverStoredprocedures()
    {
        return Helper.ListStoredprocedures(Server.Builder,GetRuntimeName());
    }

    /// <summary>
    /// Returns the name of the database
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return _database;
    }

    /// <summary>
    /// Connects to the server and enumerates the databases to see whether the currently described database exists.
    /// </summary>
    /// <param name="transaction">Database level operations are usually not transaction bound so be very careful about setting a parameter for this</param>
    /// <returns></returns>
    public bool Exists(IManagedTransaction transaction = null)
    {
        return Server.DiscoverDatabases().Any(db => db.GetRuntimeName().Equals(GetRuntimeName(),StringComparison.InvariantCultureIgnoreCase));
    }

    /// <summary>
    /// Drops the database from the server (deletes).  There is no going back after calling this method unless you have a database backup.
    /// </summary>
    public void Drop()
    {
        if (!Exists())
            throw new InvalidOperationException(string.Format(FAnsiStrings.DiscoveredDatabase_DatabaseDoesNotExistSoCannotBeDropped, this));

        // Pass in a copy of ourself, the Drop can mutate the connection string which can cause nasty side-effects (because many classes, e.g. attachers, hold references to these objects)
        Helper.DropDatabase(new DiscoveredDatabase(Server, _database, _querySyntaxHelper));
    }

    /// <summary>
    /// Return key value pairs which describe attributes of the database e.g. available space, physical location etc.
    /// </summary>
    /// <returns></returns>
    public Dictionary<string,string> DescribeDatabase()
    {
        return Helper.DescribeDatabase(Server.Builder, GetRuntimeName());
    }

    /// <summary>
    /// Creates the database referenced by this object.
    /// </summary>
    /// <param name="dropFirst">True to check if the database exists first and Drop if it does</param>
    public void Create(bool dropFirst = false)
    {
        if (dropFirst && Exists())
            Drop();

        Server.CreateDatabase(GetRuntimeName());
    }

    /// <summary>
    /// Assembles and runs a CREATE TABLE sql statement and returns the table created as a <see cref="DiscoveredTable"/>.
    /// </summary>
    /// <param name="tableName">The unqualified name for the table you want to create e.g. "MyTable"</param>
    /// <param name="columns">List of columns you want in your table</param>
    /// <param name="schema">Optional - The schema (if supported by DBMS) to create in.  This is NOT the database e.g. in [MyDb].[dbo].[MyTable] the schema is "dbo".
    /// If in doubt leave blank</param>
    /// <param name="adjuster">Last minute delegate class for modifying the <paramref name="columns"/> data types prior to executing SQL</param>
    /// <returns>The table created</returns>
    public DiscoveredTable CreateTable(string tableName, DatabaseColumnRequest[] columns, string schema = null, IDatabaseColumnRequestAdjuster adjuster = null)
    {
        return CreateTable(new CreateTableArgs(this,tableName, schema)
        {
            Adjuster = adjuster,
            ExplicitColumnDefinitions = columns
        });
    }

    /// <summary>
    /// Assembles and runs a CREATE TABLE sql statement and returns the table created as a <see cref="DiscoveredTable"/>.
    /// </summary>
    /// <param name="tableName">The unqualified name for the table you want to create e.g. "MyTable"</param>
    /// <param name="columns">List of columns you want in your table</param>
    /// <param name="adjuster">Last minute delegate class for modifying the <paramref name="columns"/> data types prior to executing SQL</param>
    /// <param name="foreignKeyPairs">Creates a single foreign key between the table created (parent) and a child table.  Columns in this parameter
    ///  must be a subset of <paramref name="columns"/>
    ///
    /// Key is the foreign key column (and the table the constraint will be put on).
    /// Value is the primary key table column (which the constraint reference points to)
    /// 
    /// </param>
    /// <param name="cascadeDelete">True to set CASCADE DELETE on the foreign key created by <paramref name="foreignKeyPairs"/></param>
    /// <returns>The table created</returns>
    public DiscoveredTable CreateTable(string tableName, DatabaseColumnRequest[] columns, Dictionary<DatabaseColumnRequest, DiscoveredColumn> foreignKeyPairs, bool cascadeDelete, IDatabaseColumnRequestAdjuster adjuster = null)
    {
        return CreateTable(new CreateTableArgs(this, tableName, null, foreignKeyPairs, cascadeDelete)
        {
            Adjuster = adjuster,
            ExplicitColumnDefinitions = columns
        });

    }

    /// <summary>
    /// <para>Assembles and runs a CREATE TABLE sql statement based on the data/columns in <paramref name="dt"/> and returns the table created as a <see cref="DiscoveredTable"/>.</para>
    /// 
    /// <para>This will also INSERT the data in <paramref name="dt"/> into the table created unless <paramref name="createEmpty"/> is true</para>
    /// </summary>
    /// <param name="tableName">The unqualified name for the table you want to create e.g. "MyTable"</param>
    /// <param name="dt">Data on which to base the CREATE statement on.  Supports untyped (string) data e.g. "1", "101" "200" would create an int column</param>
    /// <param name="createEmpty">True to bulk insert the Rows in the <paramref name="dt"/> after issuing CREATE.  False to create only the empty schema</param>
    /// <param name="adjuster">Last minute delegate class for modifying the table columns data types prior to executing SQL</param>
    /// <param name="explicitColumnDefinitions">Optional - Override descisions made about columns in the <paramref name="dt"/> by specify an explicit type etc</param>
    /// <returns>The table created</returns>
    public DiscoveredTable CreateTable(string tableName, DataTable dt, DatabaseColumnRequest[] explicitColumnDefinitions = null, bool createEmpty = false,IDatabaseColumnRequestAdjuster adjuster = null)
    {
        return CreateTable(new CreateTableArgs(this, tableName, null,dt,createEmpty)
        {
            ExplicitColumnDefinitions = explicitColumnDefinitions,
            Adjuster = adjuster
        });
    }

    /// <summary>
    /// Assembles and runs a CREATE TABLE sql statement and returns the table created as a <see cref="DiscoveredTable"/>.
    /// </summary>
    public DiscoveredTable CreateTable(CreateTableArgs args)
    {
        return Helper.CreateTable(args);
    }

    /// <summary>
    /// Creates a table in the database big enough to store the supplied DataTable with appropriate types.
    /// </summary>
    /// <param name="typeDictionary">The computers used to determine column types</param>
    /// <param name="tableName"></param>
    /// <param name="dt"></param>
    /// <param name="explicitColumnDefinitions"></param>
    /// <param name="createEmpty"></param>
    /// <param name="adjuster"></param>
    /// <returns></returns>
    public DiscoveredTable CreateTable(out Dictionary<string, Guesser> typeDictionary, string tableName, DataTable dt, DatabaseColumnRequest[] explicitColumnDefinitions = null, bool createEmpty = false, IDatabaseColumnRequestAdjuster adjuster = null)
    {
        var args = new CreateTableArgs(this, tableName, null, dt, createEmpty)
        {
            Adjuster = adjuster,
            ExplicitColumnDefinitions = explicitColumnDefinitions
        };
        var table = Helper.CreateTable(args);

        if(!args.TableCreated)
            throw new Exception(FAnsiStrings.DiscoveredDatabase_CreateTableDidNotPopulateTableCreatedProperty);

        typeDictionary = args.ColumnCreationLogic;

        return table;
    }


    /// <summary>
    /// Creates a new schema within the database if the DBMS supports it (Sql Server does, MySql doesn't) and it does not already exist.  Schema
    /// is a layer below server and database but above table it groups tables within a single database.
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public void CreateSchema(string name)
    {
        Helper.CreateSchema(this,name);
    }

    /// <summary>
    /// <para>Detach this DiscoveredDatabase and returns the data path where the files are stored (local to the DBMS server).</para>
    /// 
    /// <para>NOTE: you must know how to map this data path to a shared path you can access!</para>
    /// </summary>
    /// <returns>Local drive data path where the files are stored</returns>
    public DirectoryInfo Detach()
    {
        return Helper.Detach(this);
    }

    /// <summary>
    /// Creates a local (to the DBMS server) backup of the database.  Implementations may vary but should be the simplest database type
    /// e.g. MyDb.mdf should result in an incremental backup MyDb.bak.
    /// </summary>
    /// <param name="backupName"></param>
    public void CreateBackup(string backupName)
    {
        Helper.CreateBackup(this,backupName);
    }

    /// <summary>
    /// Equality based on Server and database name
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    private bool Equals(DiscoveredDatabase other)
    {
        return Equals(Server, other.Server) && string.Equals(_database, other._database,StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Equality based on Server and database name
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;

        return Equals((DiscoveredDatabase)obj);
    }

    /// <summary>
    /// Based on Server and database name
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return ((Server != null ? Server.GetHashCode() : 0) * 397) ^ (_database != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(_database) : 0);
        }
    }
}