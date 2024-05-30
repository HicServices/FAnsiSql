using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery.Constraints;
using FAnsi.Exceptions;
using FAnsi.Naming;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace FAnsi.Discovery;

/// <summary>
/// DBMS specific implementation of all functionality that relates to interacting with existing tables (altering, dropping, truncating etc).  For table creation
/// see <see cref="DiscoveredDatabaseHelper"/>.
/// </summary>
public abstract class DiscoveredTableHelper : IDiscoveredTableHelper
{
    public abstract string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX);

    public abstract DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database);

    public abstract IDiscoveredColumnHelper GetColumnHelper();
    public virtual void DropTable(DbConnection connection, DiscoveredTable tableToDrop)
    {
        var sql = tableToDrop.TableType switch
        {
            TableType.Table => "DROP TABLE {0}",
            TableType.View => "DROP VIEW {0}",
            TableType.TableValuedFunction => throw new NotSupportedException(),
            _ => throw new ArgumentOutOfRangeException(nameof(tableToDrop), "Unknown TableType")
        };

        using var cmd = tableToDrop.GetCommand(string.Format(sql, tableToDrop.GetFullyQualifiedName()), connection);
        cmd.ExecuteNonQuery();
    }

    public abstract void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop);
    public abstract void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop);

    public virtual void AddColumn(DatabaseOperationArgs args, DiscoveredTable table, string name, string dataType, bool allowNulls)
    {
        var syntax = table.GetQuerySyntaxHelper();

        using var con = args.GetManagedConnection(table);
        using var cmd = table.Database.Server.GetCommand(
            $"ALTER TABLE {table.GetFullyQualifiedName()} ADD {syntax.EnsureWrapped(name)} {dataType} {(allowNulls ? "NULL" : "NOT NULL")}", con);
        args.ExecuteNonQuery(cmd);
    }

    public virtual int GetRowCount(DatabaseOperationArgs args, DiscoveredTable table)
    {
        using var connection = args.GetManagedConnection(table);
        using var cmd = table.Database.Server.GetCommand($"SELECT count(*) FROM {table.GetFullyQualifiedName()}", connection);
        return Convert.ToInt32(args.ExecuteScalar(cmd));
    }

    public abstract IEnumerable<DiscoveredParameter> DiscoverTableValuedFunctionParameters(DbConnection connection, DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction);

    public abstract IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture);

    public virtual void TruncateTable(DiscoveredTable discoveredTable)
    {
        var server = discoveredTable.Database.Server;
        using var con = server.GetConnection();
        con.Open();
        using var cmd = server.GetCommand($"TRUNCATE TABLE {discoveredTable.GetFullyQualifiedName()}", con);
        cmd.ExecuteNonQuery();
    }

    /// <inheritdoc/>
    public string ScriptTableCreation(DiscoveredTable table, bool dropPrimaryKeys, bool dropNullability, bool convertIdentityToInt, DiscoveredTable? toCreateTable = null)
    {
        var columns = new List<DatabaseColumnRequest>();

        foreach (var c in table.DiscoverColumns())
        {
            var sqlType = c.DataType.SQLType;

            if (c.IsAutoIncrement && convertIdentityToInt)
                sqlType = "int";

            var isToDifferentDatabaseType = toCreateTable != null && toCreateTable.Database.Server.DatabaseType != table.Database.Server.DatabaseType;


            //translate types
            if (isToDifferentDatabaseType)
            {
                var fromtt = table.Database.Server.GetQuerySyntaxHelper().TypeTranslater;
                var tott = toCreateTable?.Database.Server.GetQuerySyntaxHelper().TypeTranslater ?? throw new InvalidOperationException($"Unable to retrieve type translator for {toCreateTable}");

                sqlType = fromtt.TranslateSQLDBType(c.DataType.SQLType, tott);
            }

            var colRequest = new DatabaseColumnRequest(c.GetRuntimeName(), sqlType, c.AllowNulls || dropNullability)
            {
                IsPrimaryKey = c.IsPrimaryKey && !dropPrimaryKeys,
                IsAutoIncrement = c.IsAutoIncrement && !convertIdentityToInt
            };

            colRequest.AllowNulls = colRequest.AllowNulls && !colRequest.IsAutoIncrement;

            //if there is a collation
            if (!string.IsNullOrWhiteSpace(c.Collation) && (toCreateTable == null || !isToDifferentDatabaseType))
                //if the script is to be run on a database of the same type
                //then specify that the column should use the live collation
                colRequest.Collation = c.Collation;

            columns.Add(colRequest);
        }

        var destinationTable = toCreateTable ?? table;

        var schema = toCreateTable != null ? toCreateTable.Schema : table.Schema;

        return table.Database.Helper.GetCreateTableSql(destinationTable.Database, destinationTable.GetRuntimeName(), [.. columns], null, false, schema);
    }

    public virtual bool IsEmpty(DatabaseOperationArgs args, DiscoveredTable discoveredTable) => GetRowCount(args, discoveredTable) == 0;

    public virtual void RenameTable(DiscoveredTable discoveredTable, string newName, IManagedConnection connection)
    {
        if (discoveredTable.TableType != TableType.Table)
            throw new NotSupportedException(string.Format(FAnsiStrings.DiscoveredTableHelper_RenameTable_Rename_is_not_supported_for_TableType__0_, discoveredTable.TableType));

        discoveredTable.GetQuerySyntaxHelper().ValidateTableName(newName);

        using var cmd = discoveredTable.Database.Server.Helper.GetCommand(GetRenameTableSql(discoveredTable, newName), connection.Connection, connection.Transaction);
        cmd.ExecuteNonQuery();
    }

    public virtual void CreateIndex(DatabaseOperationArgs args, DiscoveredTable table, string indexName, DiscoveredColumn[] columns, bool isUnique = false)
    {
        var syntax = table.GetQuerySyntaxHelper();

        using var connection = args.GetManagedConnection(table);
        try
        {
            var unique = isUnique ? "UNIQUE" : "";
            var columnNameList = string.Join(" , ", columns.Select(c => syntax.EnsureWrapped(c.GetRuntimeName())));
            var sql =
                $"CREATE {unique}INDEX {indexName} ON {table.GetFullyQualifiedName()} ({columnNameList})";

            using var cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
            args.ExecuteNonQuery(cmd);
        }
        catch (Exception e)
        {
            throw new AlterFailedException(string.Format(FAnsiStrings.DiscoveredTableHelper_CreateIndex_Failed, table), e);
        }
    }

    public virtual void DropIndex(DatabaseOperationArgs args, DiscoveredTable table, string indexName)
    {
        using var connection = args.GetManagedConnection(table);
        try
        {

            var sql =
                $"DROP INDEX {indexName} ON {table.GetFullyQualifiedName()}";

            using var cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
            args.ExecuteNonQuery(cmd);
        }
        catch (Exception e)
        {
            throw new AlterFailedException(string.Format(FAnsiStrings.DiscoveredTableHelper_DropIndex_Failed, table), e);
        }
    }

    public virtual void CreatePrimaryKey(DatabaseOperationArgs args, DiscoveredTable table, DiscoveredColumn[] discoverColumns)
    {
        var syntax = table.GetQuerySyntaxHelper();

        using var connection = args.GetManagedConnection(table);
        try
        {

            var sql =
                $"ALTER TABLE {table.GetFullyQualifiedName()} ADD PRIMARY KEY ({string.Join(",", discoverColumns.Select(c => syntax.EnsureWrapped(c.GetRuntimeName())))})";

            using var cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
            args.ExecuteNonQuery(cmd);
        }
        catch (Exception e)
        {
            throw new AlterFailedException(string.Format(FAnsiStrings.DiscoveredTableHelper_CreatePrimaryKey_Failed_to_create_primary_key_on_table__0__using_columns___1__, table, string.Join(",", discoverColumns.Select(static c => c.GetRuntimeName()))), e);
        }
    }

    public virtual int ExecuteInsertReturningIdentity(DiscoveredTable discoveredTable, DbCommand cmd, IManagedTransaction? transaction = null)
    {
        cmd.CommandText += ";SELECT @@IDENTITY";

        var result = cmd.ExecuteScalar();

        if (result == DBNull.Value || result == null)
            return 0;

        return Convert.ToInt32(result);
    }

    public abstract DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection, IManagedTransaction? transaction = null);

    public virtual void FillDataTableWithTopX(DatabaseOperationArgs args, DiscoveredTable table, int topX, DataTable dt)
    {
        var sql = GetTopXSqlForTable(table, topX);

        using var con = args.GetManagedConnection(table);
        using var cmd = table.Database.Server.GetCommand(sql, con);
        using var da = table.Database.Server.GetDataAdapter(cmd);
        args.Fill(da, cmd, dt);
    }

    /// <inheritdoc/>
    public virtual DiscoveredRelationship AddForeignKey(DatabaseOperationArgs args, Dictionary<DiscoveredColumn, DiscoveredColumn> foreignKeyPairs, bool cascadeDeletes, string? constraintName = null)
    {
        var foreignTables = foreignKeyPairs.Select(static c => c.Key.Table).Distinct().ToArray();
        var primaryTables = foreignKeyPairs.Select(static c => c.Value.Table).Distinct().ToArray();

        if (primaryTables.Length != 1 || foreignTables.Length != 1)
            throw new ArgumentException("Primary and foreign keys must all belong to the same table", nameof(foreignKeyPairs));


        var primary = primaryTables[0];
        var foreign = foreignTables[0];

        constraintName ??= primary.Database.Helper.GetForeignKeyConstraintNameFor(foreign, primary);

        var constraintBit = primary.Database.Helper.GetForeignKeyConstraintSql(foreign.GetRuntimeName(), primary.GetQuerySyntaxHelper(),
            foreignKeyPairs
                .ToDictionary(static k => (IHasRuntimeName)k.Key, static v => v.Value), cascadeDeletes, constraintName);

        var sql = $"""
                   ALTER TABLE {foreign.GetFullyQualifiedName()}
                                   ADD {constraintBit}
                   """;

        using (var con = args.GetManagedConnection(primary))
        {
            try
            {
                using var cmd = primary.Database.Server.GetCommand(sql, con);
                args.ExecuteNonQuery(cmd);
            }
            catch (Exception e)
            {
                throw new AlterFailedException($"Failed to create relationship using SQL:{sql}", e);
            }
        }

        return primary.DiscoverRelationships(args.TransactionIfAny).Single(
            r => r.Name.Equals(constraintName, StringComparison.CurrentCultureIgnoreCase)
        );
    }

    protected abstract string GetRenameTableSql(DiscoveredTable discoveredTable, string newName);

    public virtual void MakeDistinct(DatabaseOperationArgs args, DiscoveredTable discoveredTable)
    {
        var server = discoveredTable.Database.Server;

        //if it's got a primary key then it's distinct! job done
        if (discoveredTable.DiscoverColumns().Any(static c => c.IsPrimaryKey))
            return;

        var tableName = discoveredTable.GetFullyQualifiedName();
        var tempTable = discoveredTable.Database.ExpectTable($"{discoveredTable.GetRuntimeName()}_DistinctingTemp").GetFullyQualifiedName();


        using var con = args.TransactionIfAny == null
            ? server.BeginNewTransactedConnection()
            : //start a new transaction
            args.GetManagedConnection(server);
        using (var cmdDistinct =
               server.GetCommand(
                   string.Format("CREATE TABLE {1} AS SELECT distinct * FROM {0}", tableName, tempTable), con))
            args.ExecuteNonQuery(cmdDistinct);

        //this is the point of no return so don't cancel after this point
        using (var cmdTruncate = server.GetCommand($"DELETE FROM {tableName}", con))
        {
            cmdTruncate.CommandTimeout = args.TimeoutInSeconds;
            cmdTruncate.ExecuteNonQuery();
        }

        using (var cmdBack = server.GetCommand($"INSERT INTO {tableName} (SELECT * FROM {tempTable})", con))
        {
            cmdBack.CommandTimeout = args.TimeoutInSeconds;
            cmdBack.ExecuteNonQuery();
        }

        using (var cmdDropDistinctTable = server.GetCommand($"DROP TABLE {tempTable}", con))
        {
            cmdDropDistinctTable.CommandTimeout = args.TimeoutInSeconds;
            cmdDropDistinctTable.ExecuteNonQuery();
        }

        //if we opened a new transaction we should commit it
        if (args.TransactionIfAny == null)
            con.ManagedTransaction?.CommitAndCloseConnection();
    }

    public virtual bool RequiresLength(string columnType) =>
        columnType.ToLowerInvariant() switch
        {
            "binary" => true,
            "bit" => false,
            "char" => true,
            "image" => true,
            "nchar" => true,
            "nvarchar" => true,
            "varbinary" => true,
            "varchar" => true,
            "numeric" => true,
            _ => false
        };

    public static bool HasPrecisionAndScale(string columnType) =>
        columnType.ToLowerInvariant() switch
        {
            "decimal" => true,
            "numeric" => true,
            _ => false
        };
}