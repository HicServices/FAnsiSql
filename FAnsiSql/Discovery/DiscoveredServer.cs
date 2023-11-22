using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Exceptions;
using FAnsi.Implementation;

namespace FAnsi.Discovery;

/// <summary>
/// Cross database type reference to a database server.  Allows you to get connections, create commands, list databases etc.
/// </summary>
public class DiscoveredServer : IMightNotExist
{
    /// <summary>
    /// Stores connection string State (which server the <see cref="DiscoveredServer"/> refers to.
    /// </summary>
    public DbConnectionStringBuilder Builder { get; set; }

    /// <summary>
    /// The currently used database
    /// </summary>
    private DiscoveredDatabase _currentDatabase;

    /// <summary>
    /// Stateless helper class with DBMS specific implementation of the logic required by <see cref="DiscoveredServer"/>.
    /// </summary>
    public IDiscoveredServerHelper Helper { get; }

    /// <summary>
    /// Returns <see cref="FAnsi.DatabaseType"/> (indicates what DBMS the <see cref="DiscoveredServer"/> is pointed at).
    /// </summary>
    public DatabaseType DatabaseType => Helper.DatabaseType;

    /// <summary>
    /// The server's name as specified in <cref name="Builder"/> e.g. localhost\sqlexpress
    /// </summary>
    public string Name => Helper.GetServerName(Builder);

    /// <summary>
    /// Returns the username portion of <cref name="Builder"/> if specified
    /// </summary>
    public string ExplicitUsernameIfAny => Helper.GetExplicitUsernameIfAny(Builder);

    /// <summary>
    /// Returns the password portion of <cref name="Builder"/> if specified
    /// </summary>
    public string ExplicitPasswordIfAny => Helper.GetExplicitPasswordIfAny(Builder);

    /// <summary>
    /// <para>Creates a new server pointed at the <paramref name="builder"/> server. </para>
    /// 
    /// <para><see cref="ImplementationManager"/> must have a loaded implementation for the DBMS type</para>
    /// </summary>
    /// <param name="builder">Determines the connection string and <see cref="DatabaseType"/> e.g. MySqlConnectionStringBuilder = DatabaseType.MySql</param>
    /// <exception cref="ImplementationNotFoundException"></exception>
    public DiscoveredServer(DbConnectionStringBuilder builder)
    {
        Helper = ImplementationManager.GetImplementation(builder).GetServerHelper();

        //give helper a chance to mutilate the builder if he wants (also gives us a new copy of the builder in case anyone external modifies the old reference)
        Builder = Helper.GetConnectionStringBuilder(builder.ConnectionString);
    }

    /// <summary>
    /// <para>Creates a new server pointed at the <paramref name="connectionString"/> which should be a server of DBMS <paramref name="databaseType"/></para>
    /// 
    /// <para><see cref="ImplementationManager"/> must have a loaded implementation for the DBMS type</para>
    /// </summary>
    /// <param name="connectionString"></param>
    /// <param name="databaseType"></param>
    /// <exception cref="ImplementationNotFoundException"></exception>
    public DiscoveredServer(string connectionString, DatabaseType databaseType)
    {
        Helper = ImplementationManager.GetImplementation(databaseType).GetServerHelper();
        Builder = Helper.GetConnectionStringBuilder(connectionString);
    }

    /// <summary>
    /// <para>Creates a new server pointed at the <paramref name="server"/> which should be a server of DBMS <paramref name="databaseType"/></para>
    /// 
    /// <para><see cref="ImplementationManager"/> must have a loaded implementation for the DBMS type</para>
    /// </summary>
    /// <param name="server">The server to connect to e.g. "localhost\sqlexpress"</param>
    /// <param name="database">The default database to connect into/query (see <see cref="GetCurrentDatabase"/>)</param>
    /// <param name="databaseType">The DBMS provider type</param>
    /// <param name="usernameIfAny">Optional username to set in the connection string</param>
    /// <param name="passwordIfAny">Optional password to set in the connection string</param>
    /// <exception cref="ImplementationNotFoundException"></exception>
    public DiscoveredServer(string server,string database, DatabaseType databaseType,string usernameIfAny,string passwordIfAny)
    {
        Helper = ImplementationManager.GetImplementation(databaseType).GetServerHelper();

        Builder = Helper.GetConnectionStringBuilder(server,database,usernameIfAny,passwordIfAny);

        if(!string.IsNullOrWhiteSpace(database))
            _currentDatabase = ExpectDatabase(database);
    }


    /// <summary>
    /// Returns a new unopened connection to the server.  Use <see cref="GetCommand(string,FAnsi.Connections.IManagedConnection)"/> to start sending
    /// <see cref="DbCommand"/> without having to cast.
    /// </summary>
    /// <param name="transaction">Optional - when provided returns the <see cref="IManagedTransaction.Connection"/> instead of opening a new one </param>
    /// <returns></returns>
    public DbConnection GetConnection(IManagedTransaction transaction = null)
    {
        return transaction != null ? transaction.Connection : Helper.GetConnection(Builder);
    }

    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetCommand"]'/>
    public DbCommand GetCommand(string sql, IManagedConnection managedConnection)
    {
        var cmd = Helper.GetCommand(sql, managedConnection.Connection);
        cmd.Transaction = managedConnection.Transaction;
        return cmd;
    }

    /// <summary>
    /// Returns a new <see cref="DbCommand"/> of the correct DBMS type for the <see cref="DatabaseType"/> of the server
    /// </summary>
    /// <param name="sql">Can be null, command text for the <see cref="DbCommand"/></param>
    /// <param name="con">Correctly typed connection for the <see cref="DatabaseType"/>.  (See <see cref="GetConnection"/>)</param>
    /// <param name="transaction">Optional - if provided the <see cref="DbCommand.Transaction"/> will be set to the <paramref name="transaction"/></param>
    /// <returns></returns>
    public DbCommand GetCommand(string sql, DbConnection con, IManagedTransaction transaction = null)
    {
        var cmd = Helper.GetCommand(sql, con);

        if (transaction != null)
            cmd.Transaction = transaction.Transaction;

        return cmd;
    }

    /// <summary>
    /// Returns a new <see cref="DbParameter"/> of the current <see cref="DatabaseType"/> of the server with the given
    /// <paramref name="parameterName"/>.
    /// </summary>
    /// <param name="parameterName"></param>
    /// <returns></returns>
    private DbParameter GetParameter(string parameterName)
    {
        return Helper.GetParameter(parameterName);
    }

    /// <summary>
    /// Returns a new <see cref="DbParameter"/> of the correct <see cref="DatabaseType"/> of the server.  Also adds it
    /// to the <see cref="DbCommand.Parameters"/> of <paramref name="command"/> and sets it's <paramref name="valueForParameter"/>
    /// </summary>
    /// <param name="parameterName"></param>
    /// <param name="command"></param>
    /// <param name="valueForParameter"></param>
    /// <returns></returns>
    public DbParameter AddParameterWithValueToCommand(string parameterName, DbCommand command, object valueForParameter)
    {
        var dbParameter = GetParameter(parameterName);
        dbParameter.Value = valueForParameter;
        command.Parameters.Add(dbParameter);
        return dbParameter;
    }

    /// <summary>
    /// <para>Creates an expectation (See <see cref="IMightNotExist"/>) that there is a database with the given name on the server.
    /// This method does not query the database or confirm it exists.</para>
    /// 
    /// <para>See also <see cref="DiscoveredDatabase.Exists"/>, <see cref="DiscoveredDatabase.Create"/> etc </para>
    /// </summary>
    /// <param name="database"></param>
    /// <returns></returns>
    public DiscoveredDatabase ExpectDatabase(string database)
    {
        GetQuerySyntaxHelper().ValidateDatabaseName(database);

        var builder = Helper.ChangeDatabase(Builder, database);
        var server = new DiscoveredServer(builder);
        return new DiscoveredDatabase(server, database, Helper.GetQuerySyntaxHelper());
    }

    /// <summary>
    /// Attempts to connect to the server.  Throws <see cref="TimeoutException"/> if the server did not respond within
    /// <paramref name="timeoutInMillis"/>.
    /// </summary>
    /// <param name="timeoutInMillis"></param>
    /// <exception cref="TimeoutException"></exception>
    /// <exception cref="AggregateException"></exception>
    public void TestConnection(int timeoutInMillis = 10000)
    {
        using var con = Helper.GetConnection(Builder);
        using(var tokenSource = new CancellationTokenSource(timeoutInMillis))
        using (var openTask = con.OpenAsync(tokenSource.Token))
        {
            try
            {
                openTask.Wait(timeoutInMillis, tokenSource.Token);
            }
            catch (OperationCanceledException e)
            {
                throw new TimeoutException(
                    string.Format(
                        FAnsiStrings
                            .DiscoveredServer_TestConnection_Could_not_connect_to_server___0___after_timeout_of__1__milliseconds_,
                        Name, timeoutInMillis), e);
            }
            catch (AggregateException e)
            {
                if (openTask.IsCanceled)
                    throw new TimeoutException(
                        string.Format(
                            FAnsiStrings
                                .DiscoveredServer_TestConnection_Could_not_connect_to_server___0___after_timeout_of__1__milliseconds_,
                            Name, timeoutInMillis), e);
                throw;
            }
        }

        con.Close();
    }

    /// <summary>
    /// <para>Attempts to connect to the server giving up after <paramref name="timeoutInSeconds"/> (if supported by DBMS).</para>
    /// 
    /// <para> This differs from <see cref="TestConnection"/> in that it specifies the timeout in the connection string (if possible) and waits for the
    ///  server to shut down the connection rather than using a <see cref="CancellationToken"/>.
    /// </para>
    /// </summary>
    /// <param name="timeoutInSeconds"></param>
    /// <param name="exception"></param>
    /// <returns></returns>
    public bool RespondsWithinTime(int timeoutInSeconds, out Exception exception)
    {
        return Helper.RespondsWithinTime(Builder, timeoutInSeconds, out exception);
    }

    /// <summary>
    /// Connects to the server and returns a list of databases found as <see cref="DiscoveredDatabase"/> objects
    /// </summary>
    /// <returns></returns>
    public IEnumerable<DiscoveredDatabase> DiscoverDatabases() => Helper.ListDatabases(Builder)
        .Select(database => new DiscoveredDatabase(this, database, Helper.GetQuerySyntaxHelper())).ToArray();

    /// <summary>
    /// <para>Returns true/false based on <see cref="TestConnection"/>.  This method will use the default timeout of <see cref="TestConnection"/> (e.g. 3 seconds).</para>
    /// 
    /// <para>NOTE: Returns false if any Exception is thrown during <see cref="TestConnection"/></para>
    /// </summary>
    /// <param name="transaction">Optional - if provided this method returns true (existence cannot be checked mid transaction).</param>
    /// <returns></returns>
    public bool Exists(IManagedTransaction transaction = null)
    {
        if (transaction != null)
            return true;

        try
        {
            TestConnection();
            return true;
        }
        catch (AggregateException)
        {
            return false;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>
    /// Returns a new correctly Typed <see cref="DbDataAdapter"/> for the <see cref="DatabaseType"/>
    /// </summary>
    /// <param name="cmd"></param>
    /// <returns></returns>
    public DbDataAdapter GetDataAdapter(DbCommand cmd)
    {
        return Helper.GetDataAdapter(cmd);
    }

    /// <summary>
    /// Returns a new correctly Typed <see cref="DbDataAdapter"/> for the <see cref="DatabaseType"/>
    /// </summary>
    /// <returns></returns>
    public DbDataAdapter GetDataAdapter(string command, DbConnection con)
    {
        return GetDataAdapter(GetCommand(command, con));
    }

    /// <summary>
    /// Returns the database that <see cref="Builder"/> is currently pointed at.
    /// </summary>
    /// <returns></returns>
    public DiscoveredDatabase GetCurrentDatabase()
    {
        //Is the database name persisted in the connection string?
        var dbName = Helper.GetCurrentDatabase(Builder);

        //yes
        if(!string.IsNullOrWhiteSpace(dbName))
            return ExpectDatabase(dbName);

        //no (e.g. Oracle or no default database specified in connection string)
        return _currentDatabase; //yes use that one
    }

    /// <summary>
    /// Edits the connection string (See <see cref="Builder"/>) to allow async operations.  Depending on DBMS this may have
    /// no effect (e.g. Sql Server needs AsynchronousProcessing and MultipleActiveResultSets but Oracle / MySql do not need
    /// any special keywords)
    /// </summary>
    public void EnableAsync()
    {
        Builder = Helper.EnableAsync(Builder);
    }

    /// <summary>
    /// <para>Edits the connection string (See <see cref="Builder"/>) to open connections to the <paramref name="newDatabase"/>.</para>
    /// 
    /// <para>NOTE: Generally it is better to use <see cref="ExpectDatabase"/> instead and interact with the new object</para>
    /// </summary>
    /// <param name="newDatabase"></param>
    public void ChangeDatabase(string newDatabase)
    {
        //change the connection string to point to the newDatabase
        Builder = Helper.ChangeDatabase(Builder,newDatabase);

        //for DBMS that do not persist database in connection string (Oracle), we must persist this change
        _currentDatabase = ExpectDatabase(newDatabase);
    }

    /// <summary>
    /// Returns the server <see cref="Name"/>
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return Name ;
    }

    /// <summary>
    /// <para>Creates a new database with the given <paramref name="newDatabaseName"/>.</para>
    /// 
    /// <para>In the case of Oracle this is a user+schema (See https://stackoverflow.com/questions/880230/difference-between-a-user-and-a-schema-in-oracle) </para>
    /// </summary>
    /// <param name="newDatabaseName"></param>
    /// <returns></returns>
    public DiscoveredDatabase CreateDatabase(string newDatabaseName)
    {
        //the database we will create - it's ok DiscoveredDatabase is IMightNotExist
        var db = ExpectDatabase(newDatabaseName);

        Helper.CreateDatabase(Builder, db);

        if(!db.Exists())
            throw new Exception(string.Format(FAnsiStrings.DiscoveredServer_CreateDatabase_Helper___0___tried_to_create_database___1___but_the_database_didn_t_exist_after_the_creation_attempt, Helper.GetType().Name,newDatabaseName));

        return db;
    }

    /// <summary>
    /// Opens a new <see cref="DbConnection"/> to the server and starts a <see cref="DbTransaction"/>.  These are packaged into an <see cref="IManagedConnection"/> which
    /// should be wrapped with a using statement since it is <see cref="IDisposable"/>.
    /// </summary>
    /// <returns></returns>
    public IManagedConnection BeginNewTransactedConnection()
    {
        return new ManagedConnection(this, Helper.BeginTransaction(Builder)){CloseOnDispose = true};
    }

    /// <summary>
    /// <para>Opens a new <see cref="DbConnection"/> or reuses an existing one (if <paramref name="transaction"/> is provided).</para>
    /// 
    /// <para>The returned object should be used in a using statement since it is <see cref="IDisposable"/></para>
    /// </summary>
    /// <param name="transaction"></param>
    /// <returns></returns>
    public IManagedConnection GetManagedConnection(IManagedTransaction transaction = null)
    {
        return new ManagedConnection(this, transaction);
    }

    /// <summary>
    /// Returns helper for generating queries compatible with the DBMS (See <see cref="DatabaseType"/>) e.g. TOP X, column qualifiers, what the parameter
    /// symbol is etc.
    /// </summary>
    /// <returns></returns>
    public IQuerySyntaxHelper GetQuerySyntaxHelper()
    {
        return Helper.GetQuerySyntaxHelper();
    }

    /// <summary>
    /// Return key value pairs which describe attributes of the server e.g. version, available drive space etc
    /// </summary>
    /// <returns></returns>
    public Dictionary<string, string> DescribeServer()
    {
        return Helper.DescribeServer(Builder);
    }

    /// <summary>
    /// Equality based on Builder.ConnectionString and DatabaseType
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    protected bool Equals(DiscoveredServer other)
    {
        if (Builder == null || other.Builder == null)
            return Equals(Builder, other.Builder) && DatabaseType == other.DatabaseType;

        //server is the same if they are pointed at the same server
        return string.Equals(Builder.ConnectionString, other.Builder.ConnectionString, StringComparison.OrdinalIgnoreCase) && DatabaseType == other.DatabaseType;
    }

    /// <summary>
    /// Equality based on Builder.ConnectionString and DatabaseType
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((DiscoveredServer)obj);
    }

    /// <summary>
    /// Hashcode built from DatabaseType
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode()
    {
        return DatabaseType.GetHashCode();
    }

    /// <summary>
    /// Returns the version number of the DBMS e.g. MySql 5.7
    /// </summary>
    /// <returns></returns>
    public Version GetVersion()
    {
        return Helper.GetVersion(this);
    }
}