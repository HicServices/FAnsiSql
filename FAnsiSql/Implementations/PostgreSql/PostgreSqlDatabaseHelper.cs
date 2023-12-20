using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlDatabaseHelper : DiscoveredDatabaseHelper
{
    public static readonly PostgreSqlDatabaseHelper Instance = new();
    private PostgreSqlDatabaseHelper(){}

    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection,
        string database, bool includeViews, DbTransaction transaction = null)
    {

        const string sqlTables = """
                                 SELECT
                                                 *
                                                 FROM
                                             pg_catalog.pg_tables
                                                 WHERE
                                             schemaname != 'pg_catalog'
                                             AND schemaname != 'information_schema';
                                 """;


        const string sqlViews = """
                                SELECT
                                                *
                                                FROM
                                            pg_catalog.pg_views
                                                WHERE
                                            schemaname != 'pg_catalog'
                                            AND schemaname != 'information_schema';
                                """;

        var tables = new List<DiscoveredTable>();

        using (var cmd = new NpgsqlCommand(sqlTables, (NpgsqlConnection) connection))
        {
            cmd.Transaction = transaction as NpgsqlTransaction;

            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                //its a system table
                var schema = r["schemaname"] as string;

                if(querySyntaxHelper.IsValidTableName((string)r["tablename"], out _))
                    tables.Add(new DiscoveredTable(parent, (string)r["tablename"], querySyntaxHelper, schema));
            }
        }

        if (includeViews)
        {
            using var cmd = new NpgsqlCommand(sqlViews, (NpgsqlConnection)connection);
            cmd.Transaction = transaction as NpgsqlTransaction;

            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                //its a system table
                var schema = r["schemaname"] as string;

                if(querySyntaxHelper.IsValidTableName((string)r["viewname"], out _))
                    tables.Add(new DiscoveredTable(parent, (string)r["viewname"], querySyntaxHelper, schema, TableType.View));
            }
        }

        return tables.ToArray();
    }

    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction transaction = null) =>
        Enumerable.Empty<DiscoveredTableValuedFunction>();

    public override DiscoveredStoredprocedure[]
        ListStoredprocedures(DbConnectionStringBuilder builder, string database) =>
        Array.Empty<DiscoveredStoredprocedure>();

    public override IDiscoveredTableHelper GetTableHelper() => PostgreSqlTableHelper.Instance;

    public override void DropDatabase(DiscoveredDatabase database)
    {
        var master = database.Server.ExpectDatabase("postgres");

        NpgsqlConnection.ClearAllPools();

        using (var con = (NpgsqlConnection) master.Server.GetConnection())
        {
            con.Open();

            // https://dba.stackexchange.com/a/11895

            using(var cmd = new NpgsqlCommand($"UPDATE pg_database SET datallowconn = 'false' WHERE datname = '{database.GetRuntimeName()}';",con))
                cmd.ExecuteNonQuery();

            using(var cmd = new NpgsqlCommand($"""
                                               SELECT pg_terminate_backend(pid)
                                                               FROM pg_stat_activity
                                                               WHERE datname = '{database.GetRuntimeName()}';
                                               """
                      ,con))
                cmd.ExecuteNonQuery();

            using(var cmd = new NpgsqlCommand($"DROP DATABASE \"{database.GetRuntimeName()}\"",con))
                cmd.ExecuteNonQuery();
        }

        NpgsqlConnection.ClearAllPools();

    }

    public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
    {
        throw new NotImplementedException();
    }

    protected override string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype, IQuerySyntaxHelper syntaxHelper)
    {
        //Collations generally have to be in quotes (unless maybe they are very weird user generated ones?)

        return
            $"{syntaxHelper.EnsureWrapped(col.ColumnName)} {datatype} {(col.Default != MandatoryScalarFunctions.None ? $"default {syntaxHelper.GetScalarFunctionSql(col.Default)}" : "")} {(string.IsNullOrWhiteSpace(col.Collation) ? "" : $"COLLATE \"{col.Collation.Trim('"')}\"")} {(col.AllowNulls && !col.IsPrimaryKey ? " NULL" : " NOT NULL")} {(col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : "")}";
    }

    public override DirectoryInfo Detach(DiscoveredDatabase database)
    {
        throw new NotImplementedException();
    }

    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        throw new NotImplementedException();
    }

    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {
        using var con = discoveredDatabase.Server.GetConnection();
        con.Open();

        var syntax = discoveredDatabase.Server.GetQuerySyntaxHelper();

        var sql = $@"create schema if not exists {syntax.EnsureWrapped(name)}";

        using var cmd = discoveredDatabase.Server.GetCommand(sql, con);
        cmd.ExecuteNonQuery();
    }
}