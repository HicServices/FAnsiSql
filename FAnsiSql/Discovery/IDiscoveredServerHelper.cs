﻿using System;
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
