﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

public sealed class MySqlDatabaseHelper : DiscoveredDatabaseHelper
{
    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction? transaction = null) =>
        Enumerable.Empty<DiscoveredTableValuedFunction>();

    public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database) => throw new NotImplementedException();

    public override IDiscoveredTableHelper GetTableHelper() => MySqlTableHelper.Instance;

    public override void DropDatabase(DiscoveredDatabase database)
    {
        using var con = (MySqlConnection) database.Server.GetConnection();
        con.Open();
        using var cmd = new MySqlCommand($"DROP DATABASE `{database.GetRuntimeName()}`",con);
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
    {
        var mysqlBuilder = (MySqlConnectionStringBuilder) builder;

        return new Dictionary<string, string>
        {
            { "UserID", mysqlBuilder.UserID },
            { "Server", mysqlBuilder.Server },
            { "Database", mysqlBuilder.Database }
        };
    }

    protected override string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype, IQuerySyntaxHelper syntaxHelper) =>
        //if it is not unicode then that's fine
        col.TypeRequested?.Unicode != true ? base.GetCreateTableSqlLineForColumn(col, datatype, syntaxHelper) :
            //MySql unicode is not a data type it's a character set/collation only
            $"{syntaxHelper.EnsureWrapped(col.ColumnName)} {datatype} CHARACTER SET utf8mb4 {(col.Default != MandatoryScalarFunctions.None ? $"default {syntaxHelper.GetScalarFunctionSql(col.Default)}" : "")} COLLATE {col.Collation ?? "utf8mb4_bin"} {(col is { AllowNulls: true, IsPrimaryKey: false } ? " NULL" : " NOT NULL")} {(col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : "")}";

    public override DirectoryInfo Detach(DiscoveredDatabase database) => throw new NotImplementedException();

    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        throw new NotImplementedException();
    }

    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {

    }

    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction? transaction = null)
    {
        if (connection.State == ConnectionState.Closed)
            throw new InvalidOperationException("Expected connection to be open");

        var tables = new List<DiscoveredTable>();

        using (var cmd = new MySqlCommand($"SHOW FULL TABLES in `{database}`", (MySqlConnection) connection))
        {
            cmd.Transaction = transaction as MySqlTransaction;

            var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var isView = (string)r[1] == "VIEW";

                //if we are skipping views
                if(isView && !includeViews)
                    continue;

                //skip invalid table names
                if(!querySyntaxHelper.IsValidTableName((string)r[0],out _))
                    continue;

                tables.Add(new DiscoveredTable(parent,(string)r[0],querySyntaxHelper,null,isView ? TableType.View : TableType.Table));//this table fieldname will be something like Tables_in_mydbwhatevernameitis
            }
        }

        return tables.ToArray();
    }

}