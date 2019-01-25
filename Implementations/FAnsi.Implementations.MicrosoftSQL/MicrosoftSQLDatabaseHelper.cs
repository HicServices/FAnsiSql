﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementations.MicrosoftSQL
{
    public class MicrosoftSQLDatabaseHelper: DiscoveredDatabaseHelper
    {
        public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction transaction = null)
        {
            if (connection.State == ConnectionState.Closed)
                throw new InvalidOperationException("Expected connection to be open");

            List<DiscoveredTable> tables = new List<DiscoveredTable>();
            
            var cmd = new SqlCommand("use [" + database + "]; EXEC sp_tables", (SqlConnection)connection);
            cmd.Transaction = transaction as SqlTransaction;

            using (var r = cmd.ExecuteReader())
                while (r.Read())
                {
                    //its a system table
                    string schema = r["TABLE_OWNER"] as string;
                        
                    //its a system table
                    if (schema == "sys")
                        continue;

                    if (schema == "INFORMATION_SCHEMA")
                        continue;

                    //add views if we are including them
                    if (includeViews && r["TABLE_TYPE"].Equals("VIEW"))
                        tables.Add(new DiscoveredTable(parent, (string)r["TABLE_NAME"], querySyntaxHelper, schema, TableType.View));

                    //add tables
                    if (r["TABLE_TYPE"].Equals("TABLE"))
                        tables.Add(new DiscoveredTable(parent, (string)r["TABLE_NAME"], querySyntaxHelper, schema, TableType.Table));
                }
            
            return tables.ToArray();
        }

        public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, DbTransaction transaction = null)
        {
            List<DiscoveredTableValuedFunction> functionsToReturn = new List<DiscoveredTableValuedFunction>();

            DbCommand cmd = new SqlCommand("use [" + database + @"];select name,
 (select name from sys.schemas s where s.schema_id = o.schema_id) as schema_name
  from sys.objects o
WHERE type_desc = 'SQL_TABLE_VALUED_FUNCTION' OR type_desc ='CLR_TABLE_VALUED_FUNCTION'", (SqlConnection)connection);

            cmd.Transaction = transaction;

            using (DbDataReader r = cmd.ExecuteReader())
                while (r.Read())
                {
                    string schema = r["schema_name"] as string;

                    if (string.Equals("dbo", schema))
                        schema = null;
                    functionsToReturn.Add(new DiscoveredTableValuedFunction(parent, r["name"].ToString(), querySyntaxHelper,schema));

                }


            return functionsToReturn.ToArray();
        }

        public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database)
        {
            List<DiscoveredStoredprocedure> toReturn = new List<DiscoveredStoredprocedure>();

            using (var con = new SqlConnection(builder.ConnectionString))
            {
                con.Open();
                SqlCommand cmdFindStoredprocedure = new SqlCommand("use [" + database + @"];  SELECT * FROM sys.procedures", con);

                var result = cmdFindStoredprocedure.ExecuteReader();

                while (result.Read())
                    toReturn.Add(new DiscoveredStoredprocedure((string)result["name"]));
            }

            return toReturn.ToArray();
        }

        public override IDiscoveredTableHelper GetTableHelper()
        {
            return new MicrosoftSQLTableHelper();
        }

        public override void DropDatabase(DiscoveredDatabase database)
        {
            bool userIsCurrentlyInDatabase = database.Server.GetCurrentDatabase().GetRuntimeName().Equals(database.GetRuntimeName());

            var serverConnectionBuilder = new SqlConnectionStringBuilder(database.Server.Builder.ConnectionString);
            if (userIsCurrentlyInDatabase)
                serverConnectionBuilder.InitialCatalog = "master";

            // Create a new server so we don't mutate database.Server and cause a whole lot of side-effects in other code, e.g. attachers
            var server = new DiscoveredServer(serverConnectionBuilder);
            var databaseToDrop = database.GetRuntimeName();

            string sql = "ALTER DATABASE [" + databaseToDrop + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE" + Environment.NewLine;
            sql += "DROP DATABASE [" + databaseToDrop + "]";

            using (var con = (SqlConnection) server.GetConnection())
            {
                con.Open();
                var cmd = new SqlCommand(sql, con);
                cmd.ExecuteNonQuery();
            }
        }

        public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
        {
            using (var con = new SqlConnection(builder.ConnectionString))
            {
                con.Open();
                con.ChangeDatabase(database);
                SqlCommand cmd = new SqlCommand("exec sp_spaceused", con);

                DataSet ds = new DataSet();

                new SqlDataAdapter(cmd).Fill(ds);

                var toReturn = new Dictionary<string, string>();

                toReturn.Add(ds.Tables[0].Columns[0].ColumnName, ds.Tables[0].Rows[0][0].ToString());
                toReturn.Add(ds.Tables[0].Columns[1].ColumnName, ds.Tables[1].Rows[0][1].ToString());

                toReturn.Add(ds.Tables[1].Columns[0].ColumnName, ds.Tables[1].Rows[0][0].ToString());
                toReturn.Add(ds.Tables[1].Columns[1].ColumnName, ds.Tables[1].Rows[0][1].ToString());
                toReturn.Add(ds.Tables[1].Columns[2].ColumnName, ds.Tables[1].Rows[0][2].ToString());

                return toReturn;
            }
        }

        public override DirectoryInfo Detach(DiscoveredDatabase database)
        {
            const string GetDefaultSQLServerDatabaseDirectory = @"SELECT LEFT(physical_name,LEN(physical_name)-CHARINDEX('\',REVERSE(physical_name))+1) 
                        FROM sys.master_files mf   
                        INNER JOIN sys.[databases] d   
                        ON mf.[database_id] = d.[database_id]   
                        WHERE d.[name] = 'master' AND type = 0";

            string dataFolder;

            // Create a new server so we don't mutate database.Server and cause a whole lot of side-effects in other code, e.g. attachers
            var server = database.Server;
            var databaseToDetach = database.GetRuntimeName();

            // set in simple recovery and truncate all logs!
            string sql = "ALTER DATABASE [" + databaseToDetach + "] SET RECOVERY SIMPLE; " + Environment.NewLine + 
                         "DBCC SHRINKFILE ([" + databaseToDetach + "], 1)";
            using (var con = (SqlConnection)server.GetConnection())
            {
                con.Open();
                var cmd = new SqlCommand(sql, con);
                cmd.ExecuteNonQuery();
            }

            // other operations must be done on master
            server.ChangeDatabase("master");
            
            // set single user before detaching
            sql = "ALTER DATABASE [" + databaseToDetach + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;";
            using (var con = (SqlConnection)server.GetConnection())
            {
                con.Open();
                var cmd = new SqlCommand(sql, con);
                cmd.ExecuteNonQuery();
            }

            // detach!
            sql = @"EXEC sys.sp_detach_db '" + databaseToDetach + "';";
            using (var con = (SqlConnection)server.GetConnection())
            {
                con.Open();
                var cmd = new SqlCommand(sql, con);
                cmd.ExecuteNonQuery();
            }

            // get data-files path from SQL Server
            using (var connection = (SqlConnection)server.GetConnection())
            {
                connection.Open();
                dataFolder = new SqlCommand(GetDefaultSQLServerDatabaseDirectory, connection).ExecuteScalar() as string;
            }

            return new DirectoryInfo(dataFolder);
        }

        public override void CreateBackup(DiscoveredDatabase discoveredDatabase,string backupName)
        {
            var server = discoveredDatabase.Server;
            using(var con = server.GetConnection())
            {
                con.Open();

                string sql = string.Format(
                    "BACKUP DATABASE {0} TO  DISK = '{0}.bak' WITH  INIT ,  NOUNLOAD ,  NAME = N'{1}',  NOSKIP ,  STATS = 10,  NOFORMAT",
                    discoveredDatabase.GetRuntimeName(),backupName);

                var cmd = server.GetCommand(sql,con);
                cmd.ExecuteNonQuery();
            }
        }
    }
}
