﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlServerHelper : DiscoveredServerHelper
{
    public static readonly PostgreSqlServerHelper Instance = new();

    private PostgreSqlServerHelper() : base(DatabaseType.PostgreSql)
    {
    }

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString);

    protected override string ServerKeyName => "Host";
    protected override string DatabaseKeyName => "Database";
    protected override string ConnectionTimeoutKeyName => "Timeout";


    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder) =>
        //nothing special we need to turn on
        builder;

    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => PostgreSqlDatabaseHelper.Instance;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => PostgreSqlSyntaxHelper.Instance;

    public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
    {
        using var con = new NpgsqlConnection(builder.ConnectionString);
        con.Open();
        using var cmd = GetCommand($"CREATE DATABASE \"{newDatabaseName.GetRuntimeName()}\"", con);
        cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder) => throw new NotImplementedException();

    public override string? GetExplicitUsernameIfAny(DbConnectionStringBuilder builder) => ((NpgsqlConnectionStringBuilder)builder).Username;

    public override string? GetExplicitPasswordIfAny(DbConnectionStringBuilder builder) => ((NpgsqlConnectionStringBuilder)builder).Password;

    public override Version? GetVersion(DiscoveredServer server)
    {
        using var con = new NpgsqlConnection(server.Builder.ConnectionString);
        con.Open();
        return con.PostgreSqlVersion;
    }


    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        using var con = new NpgsqlConnection(builder.ConnectionString);
        con.Open();
        foreach (var listDatabase in ListDatabases(con)) yield return listDatabase;
    }

    public override IEnumerable<string> ListDatabases(DbConnection con)
    {
        using var cmd = GetCommand("SELECT datname FROM pg_database;", con);
        using var r = cmd.ExecuteReader();
        while (r.Read())
            yield return (string)r["datname"];
    }

    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) => new NpgsqlCommand(s, (NpgsqlConnection)
        con, (NpgsqlTransaction?)transaction);

    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => new NpgsqlDataAdapter((NpgsqlCommand)cmd);

    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) => new NpgsqlCommandBuilder(new NpgsqlDataAdapter((NpgsqlCommand)cmd));

    public override DbParameter GetParameter(string parameterName) => new NpgsqlParameter { ParameterName = parameterName };

    public override DbConnection GetConnection(DbConnectionStringBuilder builder) => new NpgsqlConnection(builder.ConnectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string? database,
        string username, string password)
    {
        var toReturn = new NpgsqlConnectionStringBuilder
        {
            Host = server
        };

        if (!string.IsNullOrWhiteSpace(username))
        {
            toReturn.Username = username;
            toReturn.Password = password;
        }

        if (!string.IsNullOrWhiteSpace(database))
            toReturn.Database = database;

        return toReturn;
    }
}