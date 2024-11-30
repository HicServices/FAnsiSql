using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Exceptions;
using FAnsi.Naming;
using Microsoft.Data.SqlClient;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed partial class MicrosoftSQLTableHelper : DiscoveredTableHelper
{
    public override IEnumerable<DiscoveredColumn> DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
    {
        //don't bother looking for pks if it is a table valued function
        var pks = discoveredTable is DiscoveredTableValuedFunction
            ? null
            : ListPrimaryKeys(connection, discoveredTable).ToHashSet();

        using var cmd = discoveredTable.GetCommand(
            $"use [{database}];\r\nSELECT  \r\nsys.columns.name AS COLUMN_NAME,\r\n sys.types.name AS TYPE_NAME,\r\n  sys.columns.collation_name AS COLLATION_NAME,\r\n   sys.columns.max_length as LENGTH,\r\n   sys.columns.scale as SCALE,\r\n    sys.columns.is_identity,\r\n    sys.columns.is_nullable,\r\n   sys.columns.precision as PRECISION,\r\nsys.columns.collation_name\r\nfrom sys.columns \r\njoin \r\nsys.types on sys.columns.user_type_id = sys.types.user_type_id\r\nwhere object_id = OBJECT_ID(@tableName)", connection.Connection, connection.Transaction);
        var p = cmd.CreateParameter();
        p.ParameterName = "@tableName";
        p.Value = GetObjectName(discoveredTable);
        cmd.Parameters.Add(p);

        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var isNullable = Convert.ToBoolean(r["is_nullable"]);

            //if it is a table valued function prefix the column name with the table valued function name
            var columnName = discoveredTable is DiscoveredTableValuedFunction
                ? $"{discoveredTable.GetRuntimeName()}.{r["COLUMN_NAME"]}"
                : r["COLUMN_NAME"].ToString();

            var toAdd = new DiscoveredColumn(discoveredTable,
                columnName ?? throw new InvalidOperationException("Anonymous column found"), isNullable)
            {
                IsAutoIncrement = Convert.ToBoolean(r["is_identity"]),
                Collation = r["collation_name"] as string
            };
            toAdd.DataType = new DiscoveredDataType(r, GetSQLType_FromSpColumnsResult(r), toAdd);
            toAdd.IsPrimaryKey = pks?.Contains(toAdd.GetRuntimeName()) ?? false;
            yield return toAdd;
        }
    }

    /// <summary>
    /// Returns the table name suitable for being passed into OBJECT_ID including schema if any
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static string GetObjectName(DiscoveredTable table)
    {
        var syntax = table.GetQuerySyntaxHelper();

        var objectName = syntax.EnsureWrapped(table.GetRuntimeName());

        return table.Schema != null ? $"{syntax.EnsureWrapped(table.Schema)}.{objectName}" : objectName;
    }

    public override IDiscoveredColumnHelper GetColumnHelper() => new MicrosoftSQLColumnHelper();

    public override void DropTable(DbConnection connection, DiscoveredTable tableToDrop)
    {
        SqlCommand cmd;

        switch (tableToDrop.TableType)
        {
            case TableType.View:
                if (connection.Database != tableToDrop.Database.GetRuntimeName())
                    connection.ChangeDatabase(tableToDrop.GetRuntimeName());

                if (!connection.Database.ToLower().Equals(tableToDrop.Database.GetRuntimeName().ToLower()))
                    throw new NotSupportedException(
                        $"Cannot drop view {tableToDrop} because it exists in database {tableToDrop.Database.GetRuntimeName()} while the current current database connection is pointed at database:{connection.Database} (use .ChangeDatabase on the connection first) - SQL Server does not support cross database view dropping");

                cmd = new SqlCommand($"DROP VIEW {tableToDrop.GetWrappedName()}", (SqlConnection)connection);
                break;
            case TableType.Table:
                cmd = new SqlCommand($"DROP TABLE {tableToDrop.GetFullyQualifiedName()}", (SqlConnection)connection);
                break;
            case TableType.TableValuedFunction:
                DropFunction(connection, (DiscoveredTableValuedFunction)tableToDrop);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(tableToDrop), $"Unknown table type {tableToDrop.TableType}");
        }

        using (cmd)
            cmd.ExecuteNonQuery();
    }

    public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
    {
        using var cmd = new SqlCommand($"DROP FUNCTION {functionToDrop.Schema ?? "dbo"}.{functionToDrop.GetRuntimeName()}", (SqlConnection)connection);
        cmd.ExecuteNonQuery();
    }

    public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
    {
        using var cmd = new SqlCommand(
            $"ALTER TABLE {columnToDrop.Table.GetFullyQualifiedName()} DROP column {columnToDrop.GetWrappedName()}", (SqlConnection)connection);
        cmd.ExecuteNonQuery();
    }


    public override IEnumerable<DiscoveredParameter> DiscoverTableValuedFunctionParameters(DbConnection connection,
        DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction? transaction)
    {
        if (connection.State != ConnectionState.Open)
            throw new ArgumentException($@"Connection state was {connection.State} but had to be Open", nameof(connection));

        const string query = """
                             select
                             sys.parameters.name AS name,
                             sys.types.name AS TYPE_NAME,
                             sys.parameters.max_length AS LENGTH,
                             sys.types.collation_name AS COLLATION_NAME,
                             sys.parameters.scale AS SCALE,
                             sys.parameters.precision AS PRECISION
                              from
                             sys.parameters
                             join
                             sys.types on sys.parameters.user_type_id = sys.types.user_type_id
                             where object_id = OBJECT_ID(@tableName)
                             """;

        using var cmd = discoveredTableValuedFunction.GetCommand(query, connection);
        var p = cmd.CreateParameter();
        p.ParameterName = "@tableName";
        p.Value = GetObjectName(discoveredTableValuedFunction);
        cmd.Parameters.Add(p);

        cmd.Transaction = transaction;

        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var name = r["name"].ToString();
            if (name != null)
                yield return new DiscoveredParameter(name)
                {
                    DataType = new DiscoveredDataType(r, GetSQLType_FromSpColumnsResult(r), null)
                };
        }
    }

    public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture) => new MicrosoftSQLBulkCopy(discoveredTable, connection, culture);

    public override void CreatePrimaryKey(DatabaseOperationArgs args, DiscoveredTable table, DiscoveredColumn[] discoverColumns)
    {
        try
        {
            using var connection = args.GetManagedConnection(table);
            var columnHelper = GetColumnHelper();
            foreach (var alterSql in discoverColumns.Where(static dc => dc.AllowNulls).Select(col =>
                         columnHelper.GetAlterColumnToSql(col,
                             col.DataType?.SQLType ?? throw new InvalidOperationException("Missing type"), false)))
            {
                using var alterCmd = table.GetCommand(alterSql, connection.Connection, connection.Transaction);
                args.ExecuteNonQuery(alterCmd);
            }
        }
        catch (Exception e)
        {
            throw new AlterFailedException(string.Format(FAnsiStrings.DiscoveredTableHelper_CreatePrimaryKey_Failed_to_create_primary_key_on_table__0__using_columns___1__, table, string.Join(",", discoverColumns.Select(static c => c.GetRuntimeName()))), e);
        }

        base.CreatePrimaryKey(args, table, discoverColumns);
    }

    public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection, IManagedTransaction? transaction = null)
    {
        var toReturn = new Dictionary<string, DiscoveredRelationship>();

        const string sql = "exec sp_fkeys @pktable_name = @table, @pktable_qualifier=@database, @pktable_owner=@schema";

        using (var cmd = table.GetCommand(sql, connection))
        {
            if (transaction != null)
                cmd.Transaction = transaction.Transaction;

            var p = cmd.CreateParameter();
            p.ParameterName = "@table";
            p.Value = table.GetRuntimeName();
            p.DbType = DbType.String;
            cmd.Parameters.Add(p);

            p = cmd.CreateParameter();
            p.ParameterName = "@schema";
            p.Value = table.Schema ?? "dbo";
            p.DbType = DbType.String;
            cmd.Parameters.Add(p);

            p = cmd.CreateParameter();
            p.ParameterName = "@database";
            p.Value = table.Database.GetRuntimeName();
            p.DbType = DbType.String;
            cmd.Parameters.Add(p);

            using var dt = new DataTable();
            var da = table.Database.Server.GetDataAdapter(cmd);
            da.Fill(dt);

            foreach (DataRow r in dt.Rows)
            {
                var fkName = r["FK_NAME"].ToString() ?? throw new InvalidOperationException("Null foreign key name returned");

                //could be a 2+ columns foreign key?
                if (!toReturn.TryGetValue(fkName, out var current))
                {
                    var pkdb = r["PKTABLE_QUALIFIER"].ToString() ?? throw new InvalidOperationException("Null primary key database name returned");
                    var pkschema = r["PKTABLE_OWNER"].ToString();
                    var pktableName = r["PKTABLE_NAME"].ToString() ?? throw new InvalidOperationException("Null primary key table name returned");

                    var pktable = table.Database.Server.ExpectDatabase(pkdb).ExpectTable(pktableName, pkschema);

                    var fkdb = r["FKTABLE_QUALIFIER"].ToString() ?? throw new InvalidOperationException("Null foreign key database name returned");
                    var fkschema = r["FKTABLE_OWNER"].ToString();
                    var fktableName = r["FKTABLE_NAME"].ToString() ?? throw new InvalidOperationException("Null foreign key name returned");

                    var fktable = table.Database.Server.ExpectDatabase(fkdb).ExpectTable(fktableName, fkschema);

                    var deleteRuleInt = Convert.ToInt32(r["DELETE_RULE"]);

                    var deleteRule = deleteRuleInt switch
                    {
                        0 => CascadeRule.Delete,
                        1 => CascadeRule.NoAction,
                        2 => CascadeRule.SetNull,
                        3 => CascadeRule.SetDefault,
                        _ => CascadeRule.Unknown
                    };

                    /*
    https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-fkeys-transact-sql?view=sql-server-2017
                         
    0=CASCADE changes to foreign key.
    1=NO ACTION changes if foreign key is present.
    2 = set null
    3 = set default*/

                    current = new DiscoveredRelationship(fkName, pktable, fktable, deleteRule);
                    toReturn.Add(current.Name, current);
                }

                current.AddKeys(
                    r["PKCOLUMN_NAME"].ToString() ?? throw new InvalidOperationException("Unnamed primary key column"),
                    r["FKCOLUMN_NAME"].ToString() ?? throw new InvalidOperationException("Unnamed foreign key column"), transaction);
            }
        }

        return [.. toReturn.Values];

    }

    protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
    {
        var oldName = discoveredTable.GetWrappedName();

        var syntax = discoveredTable.GetQuerySyntaxHelper();

        if (!string.IsNullOrWhiteSpace(discoveredTable.Schema))
            oldName = $"{syntax.EnsureWrapped(discoveredTable.Schema)}.{oldName}";

        return $"exec sp_rename '{syntax.Escape(oldName)}', '{syntax.Escape(newName)}'";
    }

    public override void MakeDistinct(DatabaseOperationArgs args, DiscoveredTable discoveredTable)
    {
        var syntax = discoveredTable.GetQuerySyntaxHelper();

        const string sql = """
                           DELETE f
                                       FROM (
                                       SELECT	ROW_NUMBER() OVER (PARTITION BY {0} ORDER BY {0}) AS RowNum
                                       FROM {1}
                                       
                                       ) as f
                                       where RowNum > 1
                           """;

        var columnList = string.Join(",",
            discoveredTable.DiscoverColumns().Select(c => syntax.EnsureWrapped(c.GetRuntimeName())));

        var sqlToExecute = string.Format(sql, columnList, discoveredTable.GetFullyQualifiedName());

        var server = discoveredTable.Database.Server;

        using var con = args.GetManagedConnection(server);
        using var cmd = server.GetCommand(sqlToExecute, con);
        args.ExecuteNonQuery(cmd);
    }


    public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX) => $"SELECT TOP {topX} * FROM {table.GetFullyQualifiedName()}";

    private string GetSQLType_FromSpColumnsResult(DbDataReader r)
    {
        var columnType = r["TYPE_NAME"] as string;

        if (columnType == "text")
            return "varchar(max)";

        var lengthQualifier = "";

        if (HasPrecisionAndScale(columnType ?? throw new InvalidOperationException("Null type name returned")))
            lengthQualifier = $"({r["PRECISION"]},{r["SCALE"]})";
        else if (RequiresLength(columnType)) lengthQualifier = $"({AdjustForUnicodeAndNegativeOne(columnType, Convert.ToInt32(r["LENGTH"]))})";

        return columnType + lengthQualifier;
    }

    private static object AdjustForUnicodeAndNegativeOne(string columnType, int length)
    {
        if (length == -1)
            return "max";

        if (UnicodeRegex().IsMatch(columnType))
            return length / 2;

        return length;
    }

    private static IEnumerable<string> ListPrimaryKeys(IManagedConnection con, DiscoveredTable table)
    {
        const string query = """
                             SELECT i.name AS IndexName,
                             OBJECT_NAME(ic.OBJECT_ID) AS TableName,
                             COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName,
                             c.is_identity
                             FROM sys.indexes AS i
                             INNER JOIN sys.index_columns AS ic
                             INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                             ON i.OBJECT_ID = ic.OBJECT_ID
                             AND i.index_id = ic.index_id
                             WHERE (i.is_primary_key = 1) AND ic.OBJECT_ID = OBJECT_ID(@tableName)
                             ORDER BY OBJECT_NAME(ic.OBJECT_ID), ic.key_ordinal
                             """;

        using var cmd = table.GetCommand(query, con.Connection);
        var p = cmd.CreateParameter();
        p.ParameterName = "@tableName";
        p.Value = GetObjectName(table);
        cmd.Parameters.Add(p);

        cmd.Transaction = con.Transaction;
        using var r = cmd.ExecuteReader();
        while (r.Read())
            yield return (string)r["ColumnName"];

        r.Close();
    }

    [GeneratedRegex("n(varchar|char|text)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex UnicodeRegex();
}