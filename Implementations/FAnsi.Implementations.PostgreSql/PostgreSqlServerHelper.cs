using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlServerHelper : DiscoveredServerHelper
    {

        public PostgreSqlServerHelper() : base(DatabaseType.PostgreSql)
        {
        }

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString)
        {
            return new NpgsqlConnectionStringBuilder(connectionString);
        }

        protected override string ServerKeyName => "Host";
        protected override string DatabaseKeyName => "Database";
        protected override string ConnectionTimeoutKeyName => "Timeout";


        public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder)
        {
            //nothing special we need to turn on
            return builder;
        }

        public override IDiscoveredDatabaseHelper GetDatabaseHelper()
        {
            return new PostgreSqlDatabaseHelper();
        }

        public override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new PostgreSqlSyntaxHelper();
        }

        public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
        {
            var b = (NpgsqlConnectionStringBuilder)GetConnectionStringBuilder(builder.ConnectionString);
            b.Database = null;

            using(var con = new NpgsqlConnection(b.ConnectionString))
            {
                con.Open();
                using(var cmd = GetCommand("CREATE DATABASE \"" + newDatabaseName.GetRuntimeName() + '"',con))
                    cmd.ExecuteNonQuery();
            }
        }

        public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder)
        {
            throw new NotImplementedException();
        }

        public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder)
        {
            return ((NpgsqlConnectionStringBuilder) builder).Username;
        }

        public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder)
        {
            return ((NpgsqlConnectionStringBuilder) builder).Password;
        }

        public override Version GetVersion(DiscoveredServer server)
        {
            using (var con = server.GetConnection())
            {
                con.Open();
                using (var cmd = server.GetCommand("SHOW server_version",con))
                {
                    using(var r = cmd.ExecuteReader())
                        if(r.Read())
                            return r[0] == DBNull.Value ? null: CreateVersionFromString((string)r[0]);
                        else
                            return null;
                }
            }
        }


        public override string[] ListDatabases(DbConnectionStringBuilder builder)
        {
            //create a copy so as not to corrupt the original
            var b = new NpgsqlConnectionStringBuilder(builder.ConnectionString);
            b.Database = "postgres";
            b.Timeout = 5;

            using (var con = new NpgsqlConnection(b.ConnectionString))
            {
                con.Open();
                return ListDatabases(con);
            }
        }

        public override string[] ListDatabases(DbConnection con)
        {
            List<string> databases = new List<string>();

            using(var cmd = GetCommand("SELECT datname FROM pg_database;", con))
                using(DbDataReader r = cmd.ExecuteReader())
                    while (r.Read())
                        databases.Add((string) r["datname"]);

            con.Close();
            return databases.ToArray();
        }

        public override DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null)
        {
            return new NpgsqlCommand(s, (NpgsqlConnection) con, (NpgsqlTransaction) transaction);
        }

        public override DbDataAdapter GetDataAdapter(DbCommand cmd)
        {
            return new NpgsqlDataAdapter((NpgsqlCommand) cmd);
        }

        public override DbCommandBuilder GetCommandBuilder(DbCommand cmd)
        {
            return new NpgsqlCommandBuilder(new NpgsqlDataAdapter((NpgsqlCommand) cmd));
        }

        public override DbParameter GetParameter(string parameterName)
        {
            return new NpgsqlParameter(){ParameterName = parameterName};
        }

        public override DbConnection GetConnection(DbConnectionStringBuilder builder)
        {
            return new NpgsqlConnection(builder.ConnectionString);
        }

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string database,
            string username, string password)
        {
            var toReturn = new NpgsqlConnectionStringBuilder()
            {
                Host = server
            };

            if (!string.IsNullOrWhiteSpace(username))
            {
                toReturn.Username = username;
                toReturn.Password = password;
            }
            else
                toReturn.IntegratedSecurity = true;

            if (!string.IsNullOrWhiteSpace(database))
                toReturn.Database = database;
            
            return toReturn;
        }
    }
}