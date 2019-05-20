using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;

namespace FAnsi.Discovery
{
    /// <summary>
    /// Contains all the DatabaseType specific implementation logic required by DiscoveredServer.
    /// </summary>
    public interface IDiscoveredServerHelper
    {
        DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null);
        DbDataAdapter GetDataAdapter(DbCommand cmd);
        DbCommandBuilder GetCommandBuilder(DbCommand cmd);
        DbParameter GetParameter(string parameterName);
        DbConnection GetConnection(DbConnectionStringBuilder builder);

        DbConnectionStringBuilder GetConnectionStringBuilder(string connectionString);
        
        /// <summary>
        /// Returns a new connection string builder with the supplied parameters.  Note that if a concept is not supported in the 
        /// <see cref="DbConnectionStringBuilder"/> implementation then the value will not appear in the connection string (e.g. Oracle
        /// does not support specifying a <paramref name="database"/> to connect to).
        /// </summary>
        /// <param name="server">The server/datasource to connect to e.g. "localhost\sqlexpress"</param>
        /// <param name="database">Optional database to connect to e.g. "master"</param>
        /// <param name="username">Optional username to set in connection string (otherwise integrated security will be used - if supported)</param>
        /// <param name="password">Optional password to set in connection string (otherwise integrated security will be used - if supported)</param>
        /// <returns></returns>
        DbConnectionStringBuilder GetConnectionStringBuilder(string server, string database, string username, string password);

        string GetServerName(DbConnectionStringBuilder builder);
        DbConnectionStringBuilder ChangeServer(DbConnectionStringBuilder builder, string newServer);

        string GetCurrentDatabase(DbConnectionStringBuilder builder);
        DbConnectionStringBuilder ChangeDatabase(DbConnectionStringBuilder builder, string newDatabase);

        DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder);

        string[] ListDatabases(DbConnectionStringBuilder builder);
        string[] ListDatabasesAsync(DbConnectionStringBuilder builder, CancellationToken token);

        IDiscoveredDatabaseHelper GetDatabaseHelper();
        IQuerySyntaxHelper GetQuerySyntaxHelper();

        void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName);

        ManagedTransaction BeginTransaction(DbConnectionStringBuilder builder);
        DatabaseType DatabaseType { get; }
        Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder);
        bool RespondsWithinTime(DbConnectionStringBuilder builder, int timeoutInSeconds, out Exception exception);
        
        string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder);
        string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder);
    }
}
