using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;

namespace FAnsi.Implementations.MicrosoftSQL
{

    public class MicrosoftSQLServerHelper : DiscoveredServerHelper
    {
        static MicrosoftSQLServerHelper()
        {
            //add any keywords that are required to make Oracle work properly here (at API level if it won't work period without it or SystemDefaultLow if it's just recommended)
        }

        //the name of the properties on DbConnectionStringBuilder that correspond to server and database
        public MicrosoftSQLServerHelper() : base(DatabaseType.MicrosoftSQLServer)
        {
        }

        protected override string ServerKeyName { get { return "Data Source"; }}
        protected override string DatabaseKeyName { get { return "Initial Catalog"; }}

        protected override string ConnectionTimeoutKeyName { get { return "Connect Timeout";} }

        #region Up Typing
        public override DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null)
        {
            return new SqlCommand(s, (SqlConnection)con, transaction as SqlTransaction);
        }

        public override DbDataAdapter GetDataAdapter(DbCommand cmd)
        {
            return new SqlDataAdapter((SqlCommand) cmd);
        }

        public override DbCommandBuilder GetCommandBuilder(DbCommand cmd)
        {
            return new SqlCommandBuilder((SqlDataAdapter) GetDataAdapter(cmd));
        }

        public override DbParameter GetParameter(string parameterName)
        {
            return new SqlParameter(parameterName,null);
        }

        public override DbConnection GetConnection(DbConnectionStringBuilder builder)
        {
            return new SqlConnection(builder.ConnectionString);
        }

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString)
        {
            return new SqlConnectionStringBuilder(connectionString);
        }

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string database, string username, string password)
        {
            var toReturn = new SqlConnectionStringBuilder() { DataSource = server};
            if (!string.IsNullOrWhiteSpace(username))
            {
                toReturn.UserID = username;
                toReturn.Password = password;
            }
            else
                toReturn.IntegratedSecurity = true;

            if(!string.IsNullOrWhiteSpace(database))
                toReturn.InitialCatalog = database;

            return toReturn;
        }
        public string GetDatabaseNameFrom(DbConnectionStringBuilder builder)
        {
            return ((SqlConnectionStringBuilder) builder).InitialCatalog;
        }
        #endregion

        
        public override string[] ListDatabases(DbConnectionStringBuilder builder)
        {
            //create a copy so as not to corrupt the original
            var b = new SqlConnectionStringBuilder(builder.ConnectionString);
            b.InitialCatalog = "master";
            b.ConnectTimeout = 5;

            using (var con = new SqlConnection(b.ConnectionString))
            {
                con.Open();
                return ListDatabases(con);
            }
        }

        public override string[] ListDatabases(DbConnection con)
        {
            var cmd = GetCommand("select name [Database] from master..sysdatabases", con);
            
            DbDataReader r = cmd.ExecuteReader();

            List<string> databases = new List<string>();

            while (r.Read())
                databases.Add((string) r["Database"]);

            con.Close();
            return databases.ToArray();
        }
       
        public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder)
        {
            var b = (SqlConnectionStringBuilder) builder;

            b.MultipleActiveResultSets = true;

            return b;
        }

        public override IDiscoveredDatabaseHelper GetDatabaseHelper()
        {
            return new MicrosoftSQLDatabaseHelper();
        }

        public override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new MicrosoftQuerySyntaxHelper();
        }

        public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
        {
            var b = new SqlConnectionStringBuilder(builder.ConnectionString);
            b.InitialCatalog = "master";

            using (var con = new SqlConnection(b.ConnectionString))
            {
                con.Open();
                using(SqlCommand cmd = new SqlCommand("CREATE DATABASE [" + newDatabaseName.GetRuntimeName() + "]", con))
                    cmd.ExecuteNonQuery();                
            }
        }

        public override Dictionary<string,string> DescribeServer(DbConnectionStringBuilder builder)
        {
            Dictionary<string,string> toReturn = new Dictionary<string, string>();
          
            using (SqlConnection con = new SqlConnection(builder.ConnectionString))
            {
                con.Open();
                
                //For more info you could run
                //SELECT *  FROM sys.databases WHERE name = 'AdventureWorks2012';  but there might not be a database?

                try
                {
                    using (DataTable dt = new DataTable())
                    {
                        using(var cmd = new SqlCommand("EXEC master..xp_fixeddrives",con))
                            using(var da = new SqlDataAdapter(cmd))
                                da.Fill(dt);

                        foreach (DataRow row in dt.Rows)
                            toReturn.Add("Free Space Drive" + row[0], "" + row[1]);
                    }
                }
                catch (Exception)
                {
                    toReturn.Add("Free Space ", "Unknown");
                }
            }
            

            return toReturn;
        }

        public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder)
        {
            var u = ((SqlConnectionStringBuilder) builder).UserID;
            return string.IsNullOrWhiteSpace(u) ? null: u;
        }

        public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder)
        {
            var pwd = ((SqlConnectionStringBuilder) builder).Password;
            return string.IsNullOrWhiteSpace(pwd) ? null : pwd;
        }

    }
}
