﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle
{
    public class OracleServerHelper : DiscoveredServerHelper
    {
        static OracleServerHelper()
        {
            //add any keywords that are required to make Oracle work properly here (at API level if it won't work period without it or SystemDefaultLow if it's just recommended)
        }

        public OracleServerHelper() : base(DatabaseType.Oracle)
        {
        }

        protected override string ServerKeyName { get { return "DATA SOURCE"; } }
        protected override string DatabaseKeyName { get { return "USER ID"; } }//ok is this really what oracle does?


        protected override string  ConnectionTimeoutKeyName { get { return "Connection Timeout"; } }
        #region Up Typing
        public override DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null)
        {
            return new OracleCommand(s, con as OracleConnection) {Transaction = transaction as OracleTransaction};
        }

        public override DbDataAdapter GetDataAdapter(DbCommand cmd)
        {
            return new OracleDataAdapter((OracleCommand) cmd);
        }

        public override DbCommandBuilder GetCommandBuilder(DbCommand cmd)
        {
            return new OracleCommandBuilder((OracleDataAdapter) GetDataAdapter(cmd));
        }

        public override DbParameter GetParameter(string parameterName)
        {
            return new OracleParameter(parameterName,null);
        }

        public override DbConnection GetConnection(DbConnectionStringBuilder builder)
        {
            return new OracleConnection(builder.ConnectionString);
        }

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString)
        {
            return new OracleConnectionStringBuilder(connectionString);
        }
        #endregion

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string database, string username, string password)
        {
            var toReturn = new OracleConnectionStringBuilder() {DataSource = server};

            if (string.IsNullOrWhiteSpace(username))
                toReturn.UserID = "/";
            else
            {
                toReturn.UserID = username;
                toReturn.Password = password;
            }
            
            return toReturn;
        }
        
        public override DbConnectionStringBuilder ChangeDatabase(DbConnectionStringBuilder builder, string newDatabase)
        {
            //does not apply to oracle since user = database but we create users with random passwords
            return builder;
        }

        public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder)
        {
            return builder;
        }

        public override IDiscoveredDatabaseHelper GetDatabaseHelper()
        {
            return new OracleDatabaseHelper();
        }

        public override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new OracleQuerySyntaxHelper();
        }

        public override string GetCurrentDatabase(DbConnectionStringBuilder builder)
        {
            //Oracle does not persist database as a connection string (only server).
            return null;
        }

        public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
        {
            using(var con = new OracleConnection(builder.ConnectionString))
            {
                con.Open();
                //create a new user with a random password!!! - go oracle this makes perfect sense database=user!
                using(var cmd = new OracleCommand("CREATE USER \"" + newDatabaseName.GetRuntimeName() + "\" IDENTIFIED BY pwd" +
                                                  Guid.NewGuid().ToString().Replace("-", "").Substring(0, 27) //oracle only allows 30 character passwords
                    ,con))
                    cmd.ExecuteNonQuery();

                using(var cmd = new OracleCommand("ALTER USER \"" + newDatabaseName.GetRuntimeName() + "\" quota unlimited on system", con))
                    cmd.ExecuteNonQuery();

                using(var cmd = new OracleCommand("ALTER USER \"" + newDatabaseName.GetRuntimeName() + "\" quota unlimited on users", con))
                    cmd.ExecuteNonQuery();
            }
        }

        public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder)
        {
            throw new NotImplementedException();
        }
        
        public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder)
        {
            return ((OracleConnectionStringBuilder) builder).UserID;
        }

        public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder)
        {
            return ((OracleConnectionStringBuilder)builder).Password;
        }

        public override string[] ListDatabases(DbConnectionStringBuilder builder)
        {
            //todo do we have to edit the builder in here incase it is pointed at nothing?
            using (var con = new OracleConnection(builder.ConnectionString))
            {
                con.Open();
                return ListDatabases(con);
            }
        }

        public override string[] ListDatabases(DbConnection con)
        {
            List<string> databases = new List<string>();

            using(var cmd = GetCommand("select * from all_users", con)) //already comes as single column called Database
                using (var r = cmd.ExecuteReader())
                    while (r.Read())
                        databases.Add((string) r["username"]);
            
            return databases.ToArray();
        }
    }
}