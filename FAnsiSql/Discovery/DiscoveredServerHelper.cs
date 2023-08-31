using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;

namespace FAnsi.Discovery;

/// <summary>
/// DBMS specific implementation of all functionality that relates to interacting with existing server (testing connections, creating databases, etc).
/// </summary>
public abstract class DiscoveredServerHelper:IDiscoveredServerHelper
{
    private static readonly Dictionary<DatabaseType,ConnectionStringKeywordAccumulator> ConnectionStringKeywordAccumulators = new();

    /// <summary>
    /// Register a system wide rule that all connection strings of <paramref name="databaseType"/> should include the given <paramref name="keyword"/>.
    /// </summary>
    /// <param name="databaseType"></param>
    /// <param name="keyword"></param>
    /// <param name="value"></param>
    /// <param name="priority">Resolves conflicts when multiple calls are made for the same <paramref name="keyword"/> at different times</param>
    public static void AddConnectionStringKeyword(DatabaseType databaseType, string keyword, string value,ConnectionStringKeywordPriority priority)
    {
        if(!ConnectionStringKeywordAccumulators.ContainsKey(databaseType))
            ConnectionStringKeywordAccumulators.Add(databaseType,new ConnectionStringKeywordAccumulator(databaseType));

        ConnectionStringKeywordAccumulators[databaseType].AddOrUpdateKeyword(keyword,value,priority);
    }

    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetCommand"]'/>
    public abstract DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null);

    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetDataAdapter"]'/>
    public abstract DbDataAdapter GetDataAdapter(DbCommand cmd);

    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetCommandBuilder"]'/>
    public abstract DbCommandBuilder GetCommandBuilder(DbCommand cmd);

    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetParameter"]'/>
    public abstract DbParameter GetParameter(string parameterName);

    public abstract DbConnection GetConnection(DbConnectionStringBuilder builder);

    public DbConnectionStringBuilder GetConnectionStringBuilder(string connectionString)
    {
        var builder = GetConnectionStringBuilderImpl(connectionString);
        EnforceKeywords(builder);

        return builder;
    }

    /// <inheritdoc/>
    public DbConnectionStringBuilder GetConnectionStringBuilder(string server, string database, string username, string password)
    {
        var builder = GetConnectionStringBuilderImpl(server,database,username,password);
        EnforceKeywords(builder);
        return builder;
    }

    /// <summary>
    /// Modifies the <paramref name="builder"/> with the connection string keywords
    /// specified in <see cref="ConnectionStringKeywordAccumulators"/>.  Override to
    /// perform last second changes to connection strings.
    /// </summary>
    /// <param name="builder"></param>
    protected virtual void EnforceKeywords(DbConnectionStringBuilder builder)
    {
        //if we have any keywords to enforce
        if (ConnectionStringKeywordAccumulators.TryGetValue(DatabaseType, out var accumulator))
            accumulator.EnforceOptions(builder);
    }
    protected abstract DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString, string database, string username, string password);
    protected abstract DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString);



    protected abstract string ServerKeyName { get; }
    protected abstract string DatabaseKeyName { get; }
    protected virtual string ConnectionTimeoutKeyName => "ConnectionTimeout";

    public string GetServerName(DbConnectionStringBuilder builder)
    {
        var s = (string) builder[ServerKeyName];
        return string.IsNullOrWhiteSpace(s)?null:s;
    }

    public DbConnectionStringBuilder ChangeServer(DbConnectionStringBuilder builder, string newServer)
    {
        builder[ServerKeyName] = newServer;
        return builder;
    }

    public virtual string GetCurrentDatabase(DbConnectionStringBuilder builder)
    {
        return (string) builder[DatabaseKeyName];
    }

    public virtual DbConnectionStringBuilder ChangeDatabase(DbConnectionStringBuilder builder, string newDatabase)
    {
        var newBuilder = GetConnectionStringBuilder(builder.ConnectionString);
        newBuilder[DatabaseKeyName] = newDatabase;
        return newBuilder;
    }

    public abstract IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder);
    public abstract string[] ListDatabases(DbConnection con);

    public string[] ListDatabasesAsync(DbConnectionStringBuilder builder, CancellationToken token)
    {
        //list the database on the server
        var con = GetConnection(builder);
            
        //this will work or timeout
        var openTask = con.OpenAsync(token);
        openTask.Wait(token);

        return ListDatabases(con);
    }

    public abstract DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder);

    public abstract IDiscoveredDatabaseHelper GetDatabaseHelper();
    public abstract IQuerySyntaxHelper GetQuerySyntaxHelper();

    public abstract void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName);

    public ManagedTransaction BeginTransaction(DbConnectionStringBuilder builder)
    {
        var con = GetConnection(builder);
        con.Open();
        var transaction = con.BeginTransaction();

        return new ManagedTransaction(con,transaction);
    }

    public DatabaseType DatabaseType { get; private set; }
    public abstract Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder);

    public bool RespondsWithinTime(DbConnectionStringBuilder builder, int timeoutInSeconds,out Exception exception)
    {
        try
        {
            var copyBuilder = GetConnectionStringBuilder(builder.ConnectionString);
            copyBuilder[ConnectionTimeoutKeyName] = timeoutInSeconds;

            using var con = GetConnection(copyBuilder);
            con.Open();

            con.Close();

            exception = null;
            return true;
        }
        catch (Exception e)
        {
            exception = e;
            return false;
        }
    }
    public abstract string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder);
    public abstract string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder);
    public abstract Version GetVersion(DiscoveredServer server);

    private readonly Regex rVagueVersion = new(@"\d+\.\d+(\.\d+)?(\.\d+)?",RegexOptions.Compiled|RegexOptions.CultureInvariant);

    /// <summary>
    /// Number of seconds to allow <see cref="CreateDatabase(DbConnectionStringBuilder, IHasRuntimeName)"/> to run for before timing out.
    /// Defaults to 30.
    /// </summary>
    public static int CreateDatabaseTimeoutInSeconds = 30;

    /// <summary>
    /// Returns a new <see cref="Version"/> by parsing the <paramref name="versionString"/>.  If the string
    /// is a valid version the full version string is represented otherwise a regex match is used to find
    /// numbers with dots separating them (e.g. 1.2.3  / 5.1 etc).
    /// </summary>
    /// <param name="versionString"></param>
    /// <returns></returns>
    protected Version CreateVersionFromString(string versionString)
    {
        if (Version.TryParse(versionString, out var result))
            return result;

        var m = rVagueVersion.Match(versionString);
        return m.Success ? Version.Parse(m.Value) :
            //whatever the string was it didn't even remotely resemble a Version
            null;
    }

    protected DiscoveredServerHelper(DatabaseType databaseType)
    {
        DatabaseType = databaseType;
    }
}