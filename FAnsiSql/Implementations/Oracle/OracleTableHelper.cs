using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Exceptions;
using FAnsi.Naming;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleTableHelper : DiscoveredTableHelper
{
    public static readonly OracleTableHelper Instance=new();
    private OracleTableHelper() {}

    public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX) => $"SELECT * FROM {table.GetFullyQualifiedName()} OFFSET 0 ROWS FETCH NEXT {topX} ROWS ONLY";

    public override DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
    {
        var server = discoveredTable.Database.Server;

        var columns = new List<DiscoveredColumn>();
        var tableName = discoveredTable.GetRuntimeName();

        using (var cmd = server.Helper.GetCommand("""
                                                  SELECT *
                                                  FROM   all_tab_cols
                                                  WHERE  table_name = :table_name AND owner =:owner AND HIDDEN_COLUMN <> 'YES'

                                                  """, connection.Connection))
        {
            cmd.Transaction = connection.Transaction;

            cmd.Parameters.Add(new OracleParameter("table_name", OracleDbType.Varchar2)
            {
                Value = tableName
            });
            cmd.Parameters.Add(new OracleParameter("owner", OracleDbType.Varchar2)
            {
                Value = database
            });

            using var r = cmd.ExecuteReader();
            if (!r.HasRows)
                throw new Exception($"Could not find any columns for table {tableName} in database {database}");

            while (r.Read())
            {
                var toAdd = new DiscoveredColumn(discoveredTable, (string)r["COLUMN_NAME"], r["NULLABLE"].ToString() != "N") { Format = r["CHARACTER_SET_NAME"] as string };
                toAdd.DataType = new DiscoveredDataType(r, GetSQLType_From_all_tab_cols_Result(r), toAdd);
                columns.Add(toAdd);
            }
        }


        //get auto increment information
        using (var cmd =
               new OracleCommand(
                   "select table_name,column_name from ALL_TAB_IDENTITY_COLS WHERE table_name = :table_name AND owner =:owner",
                   (OracleConnection) connection.Connection))
        {
            cmd.Transaction = (OracleTransaction?)connection.Transaction;
            cmd.Parameters.Add(new OracleParameter("table_name", OracleDbType.Varchar2)
            {
                Value = tableName
            });
            cmd.Parameters.Add(new OracleParameter("owner", OracleDbType.Varchar2)
            {
                Value = database
            });

            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var colName = r["column_name"].ToString();
                var match = columns.Single(c => c.GetRuntimeName().Equals(colName, StringComparison.CurrentCultureIgnoreCase));
                match.IsAutoIncrement = true;
            }
        }


        //get primary key information
        using(var cmd = new OracleCommand("""
                                          SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
                                          FROM all_constraints cons, all_cons_columns cols
                                          WHERE cols.table_name = :table_name AND cols.owner = :owner
                                          AND cons.constraint_type = 'P'
                                          AND cons.constraint_name = cols.constraint_name
                                          AND cons.owner = cols.owner
                                          ORDER BY cols.table_name, cols.position
                                          """, (OracleConnection) connection.Connection))
        {
            cmd.Transaction = (OracleTransaction?)connection.Transaction;
            cmd.Parameters.Add(new OracleParameter("table_name", OracleDbType.Varchar2)
            {
                Value = tableName
            });
            cmd.Parameters.Add(new OracleParameter("owner", OracleDbType.Varchar2)
            {
                Value = database
            });

            using var r = cmd.ExecuteReader();
            while (r.Read())
                columns.Single(c => c.GetRuntimeName().Equals(r["COLUMN_NAME"])).IsPrimaryKey = true;//mark all primary keys as primary
        }


        return [.. columns];
    }

    public virtual void DropIndex(DatabaseOperationArgs args, DiscoveredTable table, string indexName)
    {
        using var connection = args.GetManagedConnection(table);
        try
        {

            var sql =
                $"DROP INDEX {indexName}";

            using var cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
            args.ExecuteNonQuery(cmd);
        }
        catch (Exception e)
        {
            throw new AlterFailedException(string.Format(FAnsiStrings.DiscoveredTableHelper_DropIndex_Failed, table), e);
        }
    }

    public override IDiscoveredColumnHelper GetColumnHelper() => OracleColumnHelper.Instance;

    public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
    {
        using var cmd = new OracleCommand(
            $"ALTER TABLE {columnToDrop.Table.GetFullyQualifiedName()}  DROP COLUMN {columnToDrop.GetWrappedName()}", (OracleConnection)connection);
        cmd.ExecuteNonQuery();
    }

    private static string GetBasicTypeFromOracleType(IDataRecord r)
    {
        int? precision = null;
        int? scale = null;
        int? dataLength = null; //in bytes

        if (r["DATA_SCALE"] != DBNull.Value)
            scale = Convert.ToInt32(r["DATA_SCALE"]);
        if (r["DATA_PRECISION"] != DBNull.Value)
            precision = Convert.ToInt32(r["DATA_PRECISION"]);
        if(r["DATA_LENGTH"] != DBNull.Value)
            dataLength = Convert.ToInt32(r["DATA_LENGTH"]);

        switch (r["DATA_TYPE"] as string)
        {
            //All the ways that you can use the number keyword https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
            case "NUMBER":
                if (scale == 0 && precision == null)
                    return "int";
                if (precision != null && scale != null)
                    return "decimal";

                if (dataLength == null)
                    throw new InvalidOperationException(
                        $"Found Oracle NUMBER datatype with scale {(scale != null ? scale.ToString() : "DBNull.Value")} and precision {(precision != null ? precision.ToString() : "DBNull.Value")}, did not know what datatype to use to represent it");

                return "double";
            case "FLOAT":
                return "double";
            default:
                return r["DATA_TYPE"].ToString()?.ToLower() ?? throw new InvalidOperationException("Null DATA_TYPE in db");
        }
    }

    private string GetSQLType_From_all_tab_cols_Result(DbDataReader r)
    {
        var columnType = GetBasicTypeFromOracleType(r);

        var lengthQualifier = "";

        if (HasPrecisionAndScale(columnType))
            lengthQualifier = $"({r["DATA_PRECISION"]},{r["DATA_SCALE"]})";
        else
        if (RequiresLength(columnType))
            lengthQualifier = $"({r["DATA_LENGTH"]})";

        return columnType + lengthQualifier;
    }

    public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
    {
        throw new NotImplementedException();
    }

    public override IEnumerable<DiscoveredParameter> DiscoverTableValuedFunctionParameters(DbConnection connection,
        DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction) =>
        throw new NotImplementedException();

    public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection,CultureInfo culture) => new OracleBulkCopy(discoveredTable,connection,culture);

    public override int ExecuteInsertReturningIdentity(DiscoveredTable discoveredTable, DbCommand cmd, IManagedTransaction? transaction = null)
    {
        var autoIncrement = discoveredTable.DiscoverColumns(transaction).SingleOrDefault(static c => c.IsAutoIncrement);

        if (autoIncrement == null)
            return Convert.ToInt32(cmd.ExecuteScalar());

        var p = discoveredTable.Database.Server.Helper.GetParameter("identityOut");
        p.Direction = ParameterDirection.Output;
        p.DbType = DbType.Int32;

        cmd.Parameters.Add(p);

        cmd.CommandText += $" RETURNING {autoIncrement} INTO :identityOut;";

        cmd.CommandText =
            $"BEGIN {Environment.NewLine}{cmd.CommandText}{Environment.NewLine}COMMIT;{Environment.NewLine}END;";

        cmd.ExecuteNonQuery();


        return Convert.ToInt32(p.Value);
    }

    public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection,
        IManagedTransaction? transaction = null)
    {
        var toReturn = new Dictionary<string, DiscoveredRelationship>();

        const string sql = """

                           SELECT DISTINCT a.table_name
                                , a.column_name
                                , a.constraint_name
                                , c.owner
                                , c.delete_rule
                                , c.r_owner
                                , c_pk.table_name      r_table_name
                                , c_pk.constraint_name r_pk
                                , cc_pk.column_name    r_column_name
                             FROM all_cons_columns a
                             JOIN all_constraints  c       ON (a.owner                 = c.owner                   AND a.constraint_name   = c.constraint_name     )
                             JOIN all_constraints  c_pk    ON (c.r_owner               = c_pk.owner                AND c.r_constraint_name = c_pk.constraint_name  )
                             JOIN all_cons_columns cc_pk   on (cc_pk.constraint_name   = c_pk.constraint_name      AND cc_pk.owner         = c_pk.owner            AND cc_pk.position = a.position)
                            WHERE c.constraint_type = 'R'
                           AND  UPPER(c.r_owner) =  UPPER(:DatabaseName)
                           AND  UPPER(c_pk.table_name) =  UPPER(:TableName)
                           """;


        using (var cmd = new OracleCommand(sql, (OracleConnection) connection))
        {
            cmd.Parameters.Add(new OracleParameter(":DatabaseName", OracleDbType.Varchar2)
            {
                Value = table.Database.GetRuntimeName()
            });
            cmd.Parameters.Add(new OracleParameter(":TableName", OracleDbType.Varchar2)
            {
                Value = table.GetRuntimeName()
            });

            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var fkName = r["constraint_name"].ToString();

                //could be a 2+ columns foreign key?
                if (!toReturn.TryGetValue(fkName, out var current))
                {

                    var pkDb = r["r_owner"].ToString();
                    var pkTableName = r["r_table_name"].ToString();

                    var fkDb = r["owner"].ToString();
                    var fkTableName = r["table_name"].ToString();

                    var pktable = table.Database.Server.ExpectDatabase(pkDb).ExpectTable(pkTableName);
                    var fktable = table.Database.Server.ExpectDatabase(fkDb).ExpectTable(fkTableName);

                    //https://dev.mysql.com/doc/refman/8.0/en/referential-constraints-table.html
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

                current.AddKeys(r["r_column_name"].ToString(), r["column_name"].ToString(), transaction);
            }
        }

        return [.. toReturn.Values];
    }

    public override void FillDataTableWithTopX(DatabaseOperationArgs args,DiscoveredTable table, int topX, DataTable dt)
    {
        using var con = args.GetManagedConnection(table);
        ((OracleConnection)con.Connection).PurgeStatementCache();

        var cols = table.DiscoverColumns();

        //apparently * doesn't fly with Oracle DataAdapter
        var sql =
            $"SELECT {string.Join(",", cols.Select(static c => c.GetFullyQualifiedName()).ToArray())} FROM {table.GetFullyQualifiedName()} OFFSET 0 ROWS FETCH NEXT {topX} ROWS ONLY";

        using var cmd = table.Database.Server.GetCommand(sql, con);
        using var da = table.Database.Server.GetDataAdapter(cmd);
        args.Fill(da,cmd, dt);
    }


    protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string? newName)
    {
        newName = discoveredTable.GetQuerySyntaxHelper().EnsureWrapped(newName);
        return $@"alter table {discoveredTable.GetFullyQualifiedName()} rename to {newName}";
    }

    public override bool RequiresLength(string columnType) => base.RequiresLength(columnType) || columnType.Equals("varchar2", StringComparison.CurrentCultureIgnoreCase);
}