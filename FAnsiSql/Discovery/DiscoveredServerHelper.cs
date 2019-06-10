﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;

namespace FAnsi.Discovery
{
    /// <summary>
    /// DBMS specific implementation of all functionality that relates to interacting with existing server (testing connections, creating databases, etc).
    /// </summary>
    public abstract class DiscoveredServerHelper:IDiscoveredServerHelper
    {
        private static readonly Dictionary<DatabaseType,ConnectionStringKeywordAccumulator> ConnectionStringKeywordAccumulators = new Dictionary<DatabaseType, ConnectionStringKeywordAccumulator>();

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

        private void EnforceKeywords(DbConnectionStringBuilder builder)
        {
            //if we have any keywords to enforce
            if (ConnectionStringKeywordAccumulators.ContainsKey(DatabaseType))
                ConnectionStringKeywordAccumulators[DatabaseType].EnforceOptions(builder);
        }
        protected abstract DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString, string database, string username, string password);
        protected abstract DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString);
        
        

        protected abstract string ServerKeyName { get; }
        protected abstract string DatabaseKeyName { get; }
        protected virtual string ConnectionTimeoutKeyName { get { return "ConnectionTimeout"; } }

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

        public abstract string[] ListDatabases(DbConnectionStringBuilder builder);
        public abstract string[] ListDatabases(DbConnection con);

        public string[] ListDatabasesAsync(DbConnectionStringBuilder builder, CancellationToken token)
        {
            //list the database on the server
            DbConnection con = GetConnection(builder);
            
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

                using (var con = GetConnection(copyBuilder))
                {
                    con.Open();

                    con.Close();

                    exception = null;
                    return true;
                }
            }
            catch (Exception e)
            {
                exception = e;
                return false;
            }
        }
        public abstract string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder);
        public abstract string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder);

        protected DiscoveredServerHelper(DatabaseType databaseType)
        {
            DatabaseType = databaseType;
        }
    }
}