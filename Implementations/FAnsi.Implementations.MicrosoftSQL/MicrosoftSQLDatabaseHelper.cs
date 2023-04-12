using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.IO;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementations.MicrosoftSQL;

public class MicrosoftSQLDatabaseHelper: DiscoveredDatabaseHelper
{
    /// <summary>
    /// True to attempt sending "ALTER DATABASE MyDatabase SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
    /// before DROP DATABASE command when using <see cref="DropDatabase(DiscoveredDatabase)"/>.
    /// Defaults to true.  This command makes dropping databases more robust so is recommended but 
    /// is not supported by some servers (e.g. Microsoft Azure)
    /// </summary>
    public static bool SetSingleUserWhenDroppingDatabases = true;

    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction transaction = null)
    {
        if (connection.State == ConnectionState.Closed)
            throw new InvalidOperationException("Expected connection to be open");

        var tables = new List<DiscoveredTable>();


        using var cmd = new SqlCommand($"use {querySyntaxHelper.EnsureWrapped(database)}; EXEC sp_tables", (SqlConnection) connection);
        cmd.Transaction = transaction as SqlTransaction;

        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            //its a system table
            var schema = r["TABLE_OWNER"] as string;

            if (schema is
                //it's a system table
                "sys" or "INFORMATION_SCHEMA") continue;

            //add views if we are including them
            if (includeViews && r["TABLE_TYPE"].Equals("VIEW") &&
                querySyntaxHelper.IsValidTableName((string)r["TABLE_NAME"], out _))
                tables.Add(new DiscoveredTable(parent, (string)r["TABLE_NAME"], querySyntaxHelper, schema,
                    TableType.View));

            //add tables
            if (r["TABLE_TYPE"].Equals("TABLE") &&
                querySyntaxHelper.IsValidTableName((string)r["TABLE_NAME"], out _))
                tables.Add(new DiscoveredTable(parent, (string)r["TABLE_NAME"], querySyntaxHelper, schema));
        }

        return tables.ToArray();
    }

    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, DbTransaction transaction = null)
    {
        var functionsToReturn = new List<DiscoveredTableValuedFunction>();

        using( DbCommand cmd = new SqlCommand(
                  $"use {querySyntaxHelper.EnsureWrapped(database)};select name,\r\n (select name from sys.schemas s where s.schema_id = o.schema_id) as schema_name\r\n  from sys.objects o\r\nWHERE type_desc = 'SQL_INLINE_TABLE_VALUED_FUNCTION' OR type_desc = 'SQL_TABLE_VALUED_FUNCTION' OR type_desc ='CLR_TABLE_VALUED_FUNCTION'", (SqlConnection)connection))

        {
            cmd.Transaction = transaction;

            using (var r = cmd.ExecuteReader())
                while (r.Read())
                {
                    var schema = r["schema_name"] as string;

                    if (string.Equals("dbo", schema))
                        schema = null;
                    functionsToReturn.Add(new DiscoveredTableValuedFunction(parent, r["name"].ToString(), querySyntaxHelper,schema));

                }
        }
            

        return functionsToReturn.ToArray();
    }

    public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database)
    {
        var toReturn = new List<DiscoveredStoredprocedure>();
        var querySyntaxHelper = MicrosoftQuerySyntaxHelper.Instance;

        using var con = new SqlConnection(builder.ConnectionString);
        con.Open();
        using (var cmdFindStoredprocedure =
               new SqlCommand($"use {querySyntaxHelper.EnsureWrapped(database)};  SELECT * FROM sys.procedures", con))
        {
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
        var userIsCurrentlyInDatabase = database.Server.GetCurrentDatabase().GetRuntimeName().Equals(database.GetRuntimeName());

        var serverConnectionBuilder = new SqlConnectionStringBuilder(database.Server.Builder.ConnectionString);
        if (userIsCurrentlyInDatabase)
            serverConnectionBuilder.InitialCatalog = "master";

        // Create a new server so we don't mutate database.Server and cause a whole lot of side-effects in other code, e.g. attachers
        var server = new DiscoveredServer(serverConnectionBuilder);
        var databaseToDrop = database.GetWrappedName();

        try
        {
            // try dropping the db with single user mode enabled if the user wanted
            DropDatabase(databaseToDrop, server, SetSingleUserWhenDroppingDatabases);
        }
        catch (Exception)
        {
            // failed to drop... maybe it dropped anyway though?
            if (SetSingleUserWhenDroppingDatabases && database.Exists())
                // try without the single user mode bit
                DropDatabase(databaseToDrop, server, false);
            else
                throw;
        }
            
        SqlConnection.ClearAllPools();
    }

    /// <summary>
    /// Sends a DROP database command to the <paramref name="server"/>.  Optionally sets to SINGLE_USER 
    /// first in order to more reliably drop the database.
    /// </summary>
    /// <param name="databaseToDrop"></param>
    /// <param name="server"></param>
    /// <param name="setSingleUserModeFirst"></param>
    private void DropDatabase(string databaseToDrop, DiscoveredServer server, bool setSingleUserModeFirst)
    {
        var sql = setSingleUserModeFirst ? $"ALTER DATABASE {databaseToDrop} SET SINGLE_USER WITH ROLLBACK IMMEDIATE{Environment.NewLine}"
            : "";
        sql += $"DROP DATABASE {databaseToDrop}";

        using var con = (SqlConnection)server.GetConnection();
        con.Open();
        using var cmd = new SqlCommand(sql, con);
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
    {
        var toReturn = new Dictionary<string, string>();

        using var con = new SqlConnection(builder.ConnectionString);
        con.Open();
        con.ChangeDatabase(database);
        using var ds = new DataSet();
        using (var cmd = new SqlCommand("exec sp_spaceused", con))
        using (var da = new SqlDataAdapter(cmd))
            da.Fill(ds);

        toReturn.Add(ds.Tables[0].Columns[0].ColumnName, ds.Tables[0].Rows[0][0].ToString());
        toReturn.Add(ds.Tables[0].Columns[1].ColumnName, ds.Tables[1].Rows[0][1].ToString());

        toReturn.Add(ds.Tables[1].Columns[0].ColumnName, ds.Tables[1].Rows[0][0].ToString());
        toReturn.Add(ds.Tables[1].Columns[1].ColumnName, ds.Tables[1].Rows[0][1].ToString());
        toReturn.Add(ds.Tables[1].Columns[2].ColumnName, ds.Tables[1].Rows[0][2].ToString());

        return toReturn;
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
        var databaseToDetach = database.GetWrappedName();

        // set in simple recovery and truncate all logs!
        var sql =
            $"ALTER DATABASE {databaseToDetach} SET RECOVERY SIMPLE; {Environment.NewLine}DBCC SHRINKFILE ({databaseToDetach}, 1)";
        using (var con = (SqlConnection)server.GetConnection())
        {
            con.Open();
            using(var cmd = new SqlCommand(sql, con))
                cmd.ExecuteNonQuery();
        }

        // other operations must be done on master
        server.ChangeDatabase("master");
            
        // set single user before detaching
        sql = $"ALTER DATABASE {databaseToDetach} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;";
        using (var con = (SqlConnection)server.GetConnection())
        {
            con.Open();
            using(var cmd = new SqlCommand(sql, con))
                cmd.ExecuteNonQuery();
        }

        var dbLiteralName = database.Server.GetQuerySyntaxHelper().Escape(database.GetRuntimeName());

        // detach!
        sql = $@"EXEC sys.sp_detach_db '{dbLiteralName}';";
        using (var con = (SqlConnection)server.GetConnection())
        {
            con.Open();
            using(var cmd = new SqlCommand(sql, con))
                cmd.ExecuteNonQuery();
        }

        // get data-files path from SQL Server
        using (var connection = (SqlConnection)server.GetConnection())
        {
            connection.Open();
            using(var cmd = new SqlCommand(GetDefaultSQLServerDatabaseDirectory, connection))
                dataFolder = (string)cmd.ExecuteScalar();
        }

        return dataFolder == null ? null : new DirectoryInfo(dataFolder);
    }

    public override void CreateBackup(DiscoveredDatabase discoveredDatabase,string backupName)
    {
        var server = discoveredDatabase.Server;
        using var con = server.GetConnection();
        con.Open();

        var sql = string.Format(
            "BACKUP DATABASE {0} TO  DISK = '{0}.bak' WITH  INIT ,  NOUNLOAD ,  NAME = N'{1}',  NOSKIP ,  STATS = 10,  NOFORMAT",
            discoveredDatabase.GetWrappedName(),backupName);

        using var cmd = server.GetCommand(sql,con);
        cmd.ExecuteNonQuery();
    }

    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {
        var syntax = discoveredDatabase.Server.GetQuerySyntaxHelper();
        var runtimeName = syntax.GetRuntimeName(name);
        name = syntax.EnsureWrapped(name);

        using var con = discoveredDatabase.Server.GetConnection();
        con.Open();

        var sql = $@"if not exists (select 1 from sys.schemas where name = '{runtimeName}')
	    EXEC('CREATE SCHEMA {name}')";

        using var cmd = discoveredDatabase.Server.GetCommand(sql, con);
        cmd.ExecuteNonQuery();
    }
}