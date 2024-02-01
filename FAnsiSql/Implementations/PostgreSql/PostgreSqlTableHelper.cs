using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlTableHelper : DiscoveredTableHelper
{
    public static readonly PostgreSqlTableHelper Instance = new();
    private PostgreSqlTableHelper() {}
    public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX) => $"SELECT * FROM {table.GetFullyQualifiedName()} FETCH FIRST {topX} ROWS ONLY";

    public override DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
    {
        var toReturn = new List<DiscoveredColumn>();

        const string sqlColumns = """
                                  SELECT *
                                                  FROM information_schema.columns
                                              WHERE table_schema = @schemaName
                                              AND table_name   = @tableName;
                                  """;

        using (var cmd =
               discoveredTable.GetCommand(sqlColumns, connection.Connection, connection.Transaction))
        {
            var p = cmd.CreateParameter();
            p.ParameterName = "@tableName";
            p.Value = discoveredTable.GetRuntimeName();
            cmd.Parameters.Add(p);

            var p2 = cmd.CreateParameter();
            p2.ParameterName = "@schemaName";
            p2.Value = string.IsNullOrWhiteSpace(discoveredTable.Schema) ? PostgreSqlSyntaxHelper.DefaultPostgresSchema : discoveredTable.Schema;
            cmd.Parameters.Add(p2);


            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var isNullable = Equals(r["is_nullable"] , "YES");

                //if it is a table valued function prefix the column name with the table valued function name
                var columnName = discoveredTable is DiscoveredTableValuedFunction
                    ? $"{discoveredTable.GetRuntimeName()}.{r["column_name"]}"
                    : r["column_name"].ToString();

                var toAdd = new DiscoveredColumn(discoveredTable, columnName, isNullable)
                {
                    IsAutoIncrement = Equals(r["is_identity"],"YES"),
                    Collation = r["collation_name"] as string
                };
                toAdd.DataType = new DiscoveredDataType(r, GetSQLType_FromSpColumnsResult(r), toAdd);
                toReturn.Add(toAdd);
            }
        }



        if(toReturn.Count == 0)
            throw new Exception($"Could not find any columns in table {discoveredTable}");

        //don't bother looking for pks if it is a table valued function
        if (discoveredTable is DiscoveredTableValuedFunction)
            return [.. toReturn];

        var pks = ListPrimaryKeys(connection, discoveredTable);

        foreach (var c in toReturn.Where(c => pks.Any(pk=>pk.Equals(c.GetRuntimeName()))))
            c.IsPrimaryKey = true;

        return [.. toReturn];
    }

    private string[] ListPrimaryKeys(IManagedConnection con, DiscoveredTable table)
    {
        const string query = """
                             SELECT
                                         pg_attribute.attname,
                                         format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
                                         FROM pg_index, pg_class, pg_attribute
                                         WHERE
                                         pg_class.oid = @tableName::regclass AND
                                             indrelid = pg_class.oid AND
                                         pg_attribute.attrelid = pg_class.oid AND
                                         pg_attribute.attnum = any(pg_index.indkey)
                                         AND indisprimary
                             """;

        var toReturn = new List<string>();

        using var cmd = table.GetCommand(query, con.Connection);
        cmd.Transaction = con.Transaction;

        var p = cmd.CreateParameter();
        p.ParameterName = "@tableName";
        p.Value = table.GetFullyQualifiedName();
        cmd.Parameters.Add(p);

        using(var r = cmd.ExecuteReader())
        {
            while (r.Read())
                toReturn.Add((string) r["attname"]);

            r.Close();
        }

        return [.. toReturn];
    }

    private static string GetSQLType_FromSpColumnsResult(DbDataReader r)
    {
        var columnType = r["data_type"] as string;
        var lengthQualifier = "";

        if (HasPrecisionAndScale(columnType ?? string.Empty))
            lengthQualifier = $"({r["numeric_precision"]},{r["numeric_scale"]})";
        else if (r["character_maximum_length"] != DBNull.Value) lengthQualifier = $"({Convert.ToInt32(r["character_maximum_length"])})";

        return columnType + lengthQualifier;
    }


    public override IDiscoveredColumnHelper GetColumnHelper() => PostgreSqlColumnHelper.Instance;

    public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop) =>
        throw new NotImplementedException();

    public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
    {
        using var cmd = new NpgsqlCommand(
            $"""
             ALTER TABLE {columnToDrop.Table.GetFullyQualifiedName()}
             DROP COLUMN {columnToDrop.GetWrappedName()};
             """,(NpgsqlConnection) connection);
        cmd.ExecuteNonQuery();
    }

    public override IEnumerable<DiscoveredParameter> DiscoverTableValuedFunctionParameters(DbConnection connection,
        DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction) =>
        throw new NotImplementedException();

    public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture) => new PostgreSqlBulkCopy(discoveredTable, connection,culture);

    public override int ExecuteInsertReturningIdentity(DiscoveredTable discoveredTable, DbCommand cmd,
        IManagedTransaction? transaction = null)
    {
        var autoIncrement = discoveredTable.DiscoverColumns(transaction).SingleOrDefault(static c => c.IsAutoIncrement);

        if(autoIncrement != null)
            cmd.CommandText += $" RETURNING {autoIncrement.GetFullyQualifiedName()};";

        var result = cmd.ExecuteScalar();

        if (result == DBNull.Value || result == null)
            return 0;

        return Convert.ToInt32(result);
    }

    public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection,
        IManagedTransaction? transaction = null)
    {
        const string sql = """
                           select c.constraint_name
                               , x.table_schema as foreign_table_schema
                               , x.table_name as foreign_table_name
                               , x.column_name as foreign_column_name
                               , y.table_schema
                               , y.table_name
                               , y.column_name
                               , delete_rule
                           from information_schema.referential_constraints c
                           join information_schema.key_column_usage x
                               on x.constraint_name = c.constraint_name
                           join information_schema.key_column_usage y
                               on y.ordinal_position = x.position_in_unique_constraint
                               and y.constraint_name = c.unique_constraint_name
                           where
                               y.table_name=@tableName AND
                               y.table_schema=@schema
                           order by c.constraint_name, x.ordinal_position
                           """;


        var toReturn = new Dictionary<string, DiscoveredRelationship>();

        using (var cmd = table.GetCommand(sql, connection, transaction?.Transaction))
        {
            var p = cmd.CreateParameter();
            p.ParameterName = "@tableName";
            p.Value = table.GetRuntimeName();
            cmd.Parameters.Add(p);

            var p2 = cmd.CreateParameter();
            p2.ParameterName = "@schema";
            p2.Value = string.IsNullOrWhiteSpace(table.Schema)? PostgreSqlSyntaxHelper.DefaultPostgresSchema : table.Schema;
            cmd.Parameters.Add(p2);

            //fill data table to avoid multiple active readers
            using var dt = new DataTable();
            using(var da = new NpgsqlDataAdapter((NpgsqlCommand) cmd))
                da.Fill(dt);

            foreach(DataRow r in dt.Rows)
            {
                var fkName = r["constraint_name"].ToString();

                //could be a 2+ columns foreign key?
                if (!toReturn.TryGetValue(fkName, out var current))
                {
                    var pkDb = table.Database.GetRuntimeName();
                    var pkSchema = r["table_schema"].ToString();
                    var pkTableName = r["table_name"].ToString();

                    var fkSchema = r["foreign_table_schema"].ToString();
                    var fkTableName = r["foreign_table_name"].ToString();

                    var pktable = table.Database.Server.ExpectDatabase(pkDb).ExpectTable(pkTableName,pkSchema);
                    var fktable = table.Database.Server.ExpectDatabase(pkDb).ExpectTable(fkTableName,fkSchema);

                    var deleteRuleString = r["delete_rule"].ToString();

                    var deleteRule = deleteRuleString switch
                    {
                        "CASCADE" => CascadeRule.Delete,
                        "NO ACTION" => CascadeRule.NoAction,
                        "RESTRICT" => CascadeRule.NoAction,
                        "SET NULL" => CascadeRule.SetNull,
                        "SET DEFAULT" => CascadeRule.SetDefault,
                        _ => CascadeRule.Unknown
                    };

                    current = new DiscoveredRelationship(fkName, pktable, fktable, deleteRule);
                    toReturn.Add(current.Name, current);
                }

                current.AddKeys(r["column_name"].ToString(), r["foreign_column_name"].ToString(), transaction);
            }
        }

        return [.. toReturn.Values];
    }

    protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
    {
        var syntax = PostgreSqlSyntaxHelper.Instance;
        return $"ALTER TABLE {discoveredTable.GetFullyQualifiedName()} RENAME TO {syntax.EnsureWrapped(newName)}";
    }
}