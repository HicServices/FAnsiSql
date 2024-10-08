﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleServerHelper : DiscoveredServerHelper
{
    public static readonly OracleServerHelper Instance=new();
    private OracleServerHelper() : base(DatabaseType.Oracle)
    {
    }

    protected override string ServerKeyName => "DATA SOURCE";
    protected override string DatabaseKeyName => "USER ID"; //ok is this really what oracle does?


    protected override string  ConnectionTimeoutKeyName => "Connection Timeout";

    #region Up Typing
    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) => new OracleCommand(s, con as OracleConnection) {Transaction = transaction as OracleTransaction};

    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => new OracleDataAdapter((OracleCommand) cmd);

    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) => new OracleCommandBuilder((OracleDataAdapter) GetDataAdapter(cmd));

    public override DbParameter GetParameter(string parameterName) => new OracleParameter(parameterName,null);

    public override DbConnection GetConnection(DbConnectionStringBuilder builder) => new OracleConnection(builder.ConnectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString) =>
        new OracleConnectionStringBuilder(connectionString);

    #endregion

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string? database, string username, string password)
    {
        var toReturn = new OracleConnectionStringBuilder { DataSource = server };

        if (string.IsNullOrWhiteSpace(username))
            toReturn.UserID = "/";
        else
        {
            toReturn.UserID = username;
            toReturn.Password = password;
        }

        return toReturn;
    }

    public override DbConnectionStringBuilder ChangeDatabase(DbConnectionStringBuilder builder, string newDatabase) =>
        //does not apply to oracle since user = database but we create users with random passwords
        builder;

    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder) => builder;

    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => OracleDatabaseHelper.Instance;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => OracleQuerySyntaxHelper.Instance;

    public override string? GetCurrentDatabase(DbConnectionStringBuilder builder) =>
        //Oracle does not persist database as a connection string (only server).
        null;

    public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
    {
        using var con = new OracleConnection(builder.ConnectionString);
        con.UseHourOffsetForUnsupportedTimezone = true;
        con.Open();
        //create a new user with a random password!!! - go oracle this makes perfect sense database=user!
        using(var cmd = new OracleCommand(
                  $"CREATE USER \"{newDatabaseName.GetRuntimeName()}\" IDENTIFIED BY pwd{Guid.NewGuid().ToString().Replace("-", "")[..27]}" //oracle only allows 30 character passwords
                  , con))
        {
            cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
            cmd.ExecuteNonQuery();
        }


        using(var cmd = new OracleCommand(
                  $"ALTER USER \"{newDatabaseName.GetRuntimeName()}\" quota unlimited on system", con))
        {
            cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
            cmd.ExecuteNonQuery();
        }


        using(var cmd = new OracleCommand(
                  $"ALTER USER \"{newDatabaseName.GetRuntimeName()}\" quota unlimited on users", con))
        {
            cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
            cmd.ExecuteNonQuery();
        }
    }

    public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder) => throw new NotImplementedException();

    public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder) => ((OracleConnectionStringBuilder) builder).UserID;

    public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder) => ((OracleConnectionStringBuilder)builder).Password;

    public override Version? GetVersion(DiscoveredServer server)
    {
        using var tcon = server.GetConnection();
        if (tcon is not OracleConnection con) throw new ArgumentException("Oracle helper called on non-Oracle server",nameof(server));

        con.UseHourOffsetForUnsupportedTimezone = true;
        con.Open();
        using var cmd = server.GetCommand("SELECT * FROM v$version WHERE BANNER like 'Oracle Database%'",con);
        using var r = cmd.ExecuteReader();
        return !r.Read() || r[0] == DBNull.Value ? null: CreateVersionFromString((string)r[0]);
    }

    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        //todo do we have to edit the builder in here in case it is pointed at nothing?
        using var con = new OracleConnection(builder.ConnectionString);
        con.UseHourOffsetForUnsupportedTimezone = true;
        con.Open();
        foreach (var listDatabase in ListDatabases(con)) yield return listDatabase;
    }

    public override IEnumerable<string> ListDatabases(DbConnection con)
    {
        using var cmd = GetCommand("select * from all_users", con);
        using var r = cmd.ExecuteReader();
        while (r.Read())
            yield return (string)r["username"];
    }
}