using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

public class MySqlServerHelper : DiscoveredServerHelper
{
    public static readonly MySqlServerHelper Instance=new();
    static MySqlServerHelper()
    {
        AddConnectionStringKeyword(DatabaseType.MySql, "AllowUserVariables","True",ConnectionStringKeywordPriority.ApiRule);
        AddConnectionStringKeyword(DatabaseType.MySql, "CharSet", "utf8", ConnectionStringKeywordPriority.ApiRule);     
    }

    private MySqlServerHelper() : base(DatabaseType.MySql)
    {
    }

    protected override string ServerKeyName => "Server";
    protected override string DatabaseKeyName => "Database";

    #region Up Typing
    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null) =>
        new MySqlCommand(s, con as MySqlConnection, transaction as MySqlTransaction);

    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => new MySqlDataAdapter(cmd as MySqlCommand ??
        throw new ArgumentException("Incorrect command type", nameof(cmd)));

    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) =>
        new MySqlCommandBuilder((MySqlDataAdapter)GetDataAdapter(cmd));

    public override DbParameter GetParameter(string parameterName) => new MySqlParameter(parameterName,null);

    public override DbConnection GetConnection(DbConnectionStringBuilder builder) =>
        new MySqlConnection(builder.ConnectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString) =>
        new MySqlConnectionStringBuilder(connectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string database, string username, string password)
    {
        var toReturn = new MySqlConnectionStringBuilder
        {
            Server = server
        };

        if(!string.IsNullOrWhiteSpace(database))
            toReturn.Database = database;

        if (!string.IsNullOrWhiteSpace(username))
        {
            toReturn.UserID = username;
            toReturn.Password = password;
        }
           
        return toReturn;
    }

    #endregion

    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder) => builder; //no special stuff required?

    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => new MySqlDatabaseHelper();

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => MySqlQuerySyntaxHelper.Instance;

    public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
    {
        var b = (MySqlConnectionStringBuilder)GetConnectionStringBuilder(builder.ConnectionString);
        b.Database = null!;

        using var con = new MySqlConnection(b.ConnectionString);
        con.Open();
        using var cmd = GetCommand($"CREATE DATABASE `{newDatabaseName.GetRuntimeName()}`",con);
        cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder) => throw new NotImplementedException();

    public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder) => ((MySqlConnectionStringBuilder) builder).UserID;

    public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder) => ((MySqlConnectionStringBuilder)builder).Password;

    public override Version GetVersion(DiscoveredServer server)
    {
        using var con = server.GetConnection();
        con.Open();
        using var cmd = server.GetCommand("show variables like \"version\"",con);
        using var r = cmd.ExecuteReader();
        if (r.Read())
            return r["Value"] == DBNull.Value ? null: CreateVersionFromString((string)r["Value"]);
        return null;
    }

    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        var b = (MySqlConnectionStringBuilder)GetConnectionStringBuilder(builder.ConnectionString);
        b.Database = null!;

        using var con = new MySqlConnection(b.ConnectionString);
        con.Open();
        return ListDatabases(con);
    }
    public override string[] ListDatabases(DbConnection con)
    {
        var databases = new List<string>();

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