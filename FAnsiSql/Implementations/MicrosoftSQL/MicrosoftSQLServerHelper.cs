using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Microsoft.Data.SqlClient;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed class MicrosoftSQLServerHelper : DiscoveredServerHelper
{
    public static readonly MicrosoftSQLServerHelper Instance = new();
    private MicrosoftSQLServerHelper() : base(DatabaseType.MicrosoftSQLServer)
    {
    }

    //the name of the properties on DbConnectionStringBuilder that correspond to server and database
    protected override string ServerKeyName => "Data Source";
    protected override string DatabaseKeyName => "Initial Catalog";

    protected override string ConnectionTimeoutKeyName => "Connect Timeout";

    #region Up Typing
    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) => new SqlCommand(s, (SqlConnection)con, transaction as SqlTransaction);

    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => new SqlDataAdapter((SqlCommand)cmd);

    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) => new SqlCommandBuilder((SqlDataAdapter)GetDataAdapter(cmd));

    public override DbParameter GetParameter(string parameterName) => new SqlParameter(parameterName, null);

    public override DbConnection GetConnection(DbConnectionStringBuilder builder) => new SqlConnection(builder.ConnectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString) => new SqlConnectionStringBuilder(connectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string? database, string username, string password)
    {
        var toReturn = new SqlConnectionStringBuilder { DataSource = server };
        if (!string.IsNullOrWhiteSpace(username))
        {
            toReturn.UserID = username;
            toReturn.Password = password;
        }
        else
            toReturn.IntegratedSecurity = true;

        if (!string.IsNullOrWhiteSpace(database))
            toReturn.InitialCatalog = database;

        return toReturn;
    }
    public static string GetDatabaseNameFrom(DbConnectionStringBuilder builder) => ((SqlConnectionStringBuilder)builder).InitialCatalog;

    #endregion


    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        //create a copy so as not to corrupt the original
        var b = new SqlConnectionStringBuilder(builder.ConnectionString)
        {
            InitialCatalog = "master",
            ConnectTimeout = 5
        };

        using var con = new SqlConnection(b.ConnectionString);
        con.Open();
        return ListDatabases(con);
    }

    public override string[] ListDatabases(DbConnection con)
    {
        var databases = new List<string>();

        using (var cmd = GetCommand("select name [Database] from master..sysdatabases", con))
        using (var r = cmd.ExecuteReader())
            while (r.Read())
                databases.Add((string)r["Database"]);

        con.Close();
        return [.. databases];
    }

    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder)
    {
        var b = (SqlConnectionStringBuilder)builder;

        b.MultipleActiveResultSets = true;

        return b;
    }

    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => new MicrosoftSQLDatabaseHelper();

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => MicrosoftQuerySyntaxHelper.Instance;

    public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
    {
        var b = new SqlConnectionStringBuilder(builder.ConnectionString)
        {
            InitialCatalog = "master"
        };

        var syntax = MicrosoftQuerySyntaxHelper.Instance;


        using var con = new SqlConnection(b.ConnectionString);
        con.Open();
        using var cmd = new SqlCommand($"CREATE DATABASE {syntax.EnsureWrapped(newDatabaseName.GetRuntimeName())}", con);
        cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder)
    {
        var toReturn = new Dictionary<string, string>();

        using var con = new SqlConnection(builder.ConnectionString);
        con.Open();

        //For more info you could run
        //SELECT *  FROM sys.databases WHERE name = 'AdventureWorks2012';  but there might not be a database?

        try
        {
            using var dt = new DataTable();
            using (var cmd = new SqlCommand("EXEC master..xp_fixeddrives", con))
            using (var da = new SqlDataAdapter(cmd))
                da.Fill(dt);

            foreach (DataRow row in dt.Rows)
                toReturn.Add($"Free Space Drive{row[0]}", $"{row[1]}");
        }
        catch (Exception)
        {
            toReturn.Add("Free Space ", "Unknown");
        }


        return toReturn;
    }

    public override string? GetExplicitUsernameIfAny(DbConnectionStringBuilder builder)
    {
        var u = ((SqlConnectionStringBuilder)builder).UserID;
        return string.IsNullOrWhiteSpace(u) ? null : u;
    }

    public override string? GetExplicitPasswordIfAny(DbConnectionStringBuilder builder)
    {
        var pwd = ((SqlConnectionStringBuilder)builder).Password;
        return string.IsNullOrWhiteSpace(pwd) ? null : pwd;
    }

    protected override void EnforceKeywords(DbConnectionStringBuilder builder)
    {
        base.EnforceKeywords(builder);

        var msb = (SqlConnectionStringBuilder)builder;

        // if user has specified a keyword that indicates Azure authentication
        // then disable IntegratedSecurity
        if (msb.Authentication != SqlAuthenticationMethod.NotSpecified) msb.IntegratedSecurity = false;
    }

    public override Version? GetVersion(DiscoveredServer server)
    {
        using var con = server.GetConnection();
        con.Open();
        using var cmd = server.GetCommand("SELECT @@VERSION", con);
        using var r = cmd.ExecuteReader();
        if (r.Read())
            return r[0] == DBNull.Value ? null : CreateVersionFromString((string)r[0]);

        return null;
    }
}