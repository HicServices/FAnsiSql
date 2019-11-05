using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using MySql.Data.MySqlClient;

namespace FAnsi.Implementations.MySql
{
    public class MySqlServerHelper : DiscoveredServerHelper
    {
        static MySqlServerHelper()
        {
            AddConnectionStringKeyword(DatabaseType.MySql, "AllowUserVariables","True",ConnectionStringKeywordPriority.ApiRule);
            AddConnectionStringKeyword(DatabaseType.MySql, "AllowBatch", "True", ConnectionStringKeywordPriority.ApiRule);
            AddConnectionStringKeyword(DatabaseType.MySql, "CharSet", "utf8", ConnectionStringKeywordPriority.ApiRule);     
        }

        public MySqlServerHelper() : base(DatabaseType.MySql)
        {
        }

        protected override string ServerKeyName { get { return "Server"; } }
        protected override string DatabaseKeyName { get { return "Database"; } }

        #region Up Typing
        public override DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null)
        {
            return new MySqlCommand(s, con as MySqlConnection, transaction as MySqlTransaction);
        }

        public override DbDataAdapter GetDataAdapter(DbCommand cmd)
        {
            return new MySqlDataAdapter(cmd as MySqlCommand);
        }

        public override DbCommandBuilder GetCommandBuilder(DbCommand cmd)
        {
            return new MySqlCommandBuilder((MySqlDataAdapter) GetDataAdapter(cmd));
        }

        public override DbParameter GetParameter(string parameterName)
        {
            return new MySqlParameter(parameterName,null);
        }

        public override DbConnection GetConnection(DbConnectionStringBuilder builder)
        {
            return new MySqlConnection(builder.ConnectionString);
        }

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString)
        {
            return new MySqlConnectionStringBuilder(connectionString);
        }

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string database, string username, string password)
        {
            var toReturn = new MySqlConnectionStringBuilder()
            {
                Server = server,
            };

            if(!string.IsNullOrWhiteSpace(database))
                toReturn.Database = database;

            if (!string.IsNullOrWhiteSpace(username))
            {
                toReturn.UserID = username;
                toReturn.Password = password;
            }
            else
                toReturn.IntegratedSecurity = true;
           
            return toReturn;
        }

        #endregion
                
        public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder)
        {
            return builder; //no special stuff required?
        }

        public override IDiscoveredDatabaseHelper GetDatabaseHelper()
        {
            return new MySqlDatabaseHelper();
        }

        public override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new MySqlQuerySyntaxHelper();
        }

        public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
        {
            var b = (MySqlConnectionStringBuilder)GetConnectionStringBuilder(builder.ConnectionString);
            b.Database = null;

            using(var con = new MySqlConnection(b.ConnectionString))
            {
                con.Open();
                using(var cmd = GetCommand("CREATE DATABASE `" + newDatabaseName.GetRuntimeName() + "`",con))
                    cmd.ExecuteNonQuery();
            }
        }

        public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder)
        {
            throw new NotImplementedException();
        }
        
        public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder)
        {
            return ((MySqlConnectionStringBuilder) builder).UserID;
        }

        public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder)
        {
            return ((MySqlConnectionStringBuilder)builder).Password;
        }

        public override Version GetVersion(DiscoveredServer server)
        {
            using (var con = server.GetConnection())
            {
                con.Open();
                using (var cmd = server.GetCommand("show variables like \"version\"",con))
                {
                    using(var r = cmd.ExecuteReader())
                        if (r.Read())
                            return r["Value"] == DBNull.Value ? null: CreateVersionFromString((string)r["Value"]);
                        else
                            return null;
                }
            }
        }
        
        public override string[] ListDatabases(DbConnectionStringBuilder builder)
        {
            var b = (MySqlConnectionStringBuilder)GetConnectionStringBuilder(builder.ConnectionString);
            b.Database = null;

            using (var con = new MySqlConnection(b.ConnectionString))
            {
                con.Open();
                return ListDatabases(con);
            }
        }
        public override string[] ListDatabases(DbConnection con)
        {
            List<string> databases = new List<string>();

            using(var cmd = GetCommand("show databases;", con)) //already comes as single column called Database
                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                        databases.Add((string)r["Database"]);

                }
            
            con.Close();
            return databases.ToArray();
        }
    }
}