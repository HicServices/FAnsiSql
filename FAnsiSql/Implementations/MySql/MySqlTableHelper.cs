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
using FAnsi.Naming;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

public class MySqlTableHelper : DiscoveredTableHelper
{
    public static readonly MySqlTableHelper Instance = new();

    private MySqlTableHelper() {}

    private static readonly Regex IntParentheses = new(@"^int\(\d+\)", RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
    private static readonly Regex SmallintParentheses = new(@"^smallint\(\d+\)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex BitParentheses = new(@"^bit\(\d+\)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public override DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection,
        string database)
    {
        var columns = new List<DiscoveredColumn>();
        var tableName = discoveredTable.GetRuntimeName();

        using (var cmd = discoveredTable.Database.Server.Helper.GetCommand(
                   @"SELECT * FROM information_schema.`COLUMNS` 
WHERE table_schema = @db
  AND table_name = @tbl", connection.Connection))
        {
            cmd.Transaction = connection.Transaction;

            var p = new MySqlParameter("@db", MySqlDbType.String)
            {
                Value = discoveredTable.Database.GetRuntimeName()
            };
            cmd.Parameters.Add(p);

            p = new MySqlParameter("@tbl", MySqlDbType.String)
            {
                Value = discoveredTable.GetRuntimeName()
            };
            cmd.Parameters.Add(p);

            using var r = cmd.ExecuteReader();
            if (!r.HasRows)
                throw new Exception($"Could not find any columns for table {tableName} in database {database}");

            while (r.Read())
            {
                var toAdd = new DiscoveredColumn(discoveredTable, (string) r["COLUMN_NAME"],YesNoToBool(r["IS_NULLABLE"]));

                if (r["COLUMN_KEY"].Equals("PRI"))
                    toAdd.IsPrimaryKey = true;

                toAdd.IsAutoIncrement = r["Extra"] as string == "auto_increment";
                toAdd.Collation = r["COLLATION_NAME"] as string;

                //todo the only way to know if something in MySql is unicode is by r["character_set_name"]


                toAdd.DataType = new DiscoveredDataType(r, TrimIntDisplayValues(r["COLUMN_TYPE"].ToString()), toAdd);
                columns.Add(toAdd);

            }

            r.Close();
        }
            

        return columns.ToArray();
            
    }

    private bool YesNoToBool(object o)
    {
        if (o is bool b)
            return b;

        if (o == null || o == DBNull.Value)
            return false;

        return o.ToString() switch
        {
            "NO" => false,
            "YES" => true,
            _ => Convert.ToBoolean(o)
        };
    }



    private string TrimIntDisplayValues(string type)
    {
        //See comments of int(5) means display 5 digits only it doesn't prevent storing larger numbers: https://stackoverflow.com/a/5634147/4824531

        if (IntParentheses.IsMatch(type))
            return IntParentheses.Replace(type, "int");

        if (SmallintParentheses.IsMatch(type))
            return SmallintParentheses.Replace(type, "smallint");

        if (BitParentheses.IsMatch(type))
            return BitParentheses.Replace(type, "bit");

        return type;
    }

    public override IDiscoveredColumnHelper GetColumnHelper() => MySqlColumnHelper.Instance;

    public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
    {
        using var cmd = new MySqlCommand(
            $"alter table {columnToDrop.Table.GetFullyQualifiedName()} drop column {columnToDrop.GetWrappedName()}", (MySqlConnection)connection);
        cmd.ExecuteNonQuery();
    }


    public override IEnumerable<DiscoveredParameter> DiscoverTableValuedFunctionParameters(DbConnection connection,
        DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction)
    {
        throw new NotImplementedException();
    }

    public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable,IManagedConnection connection,CultureInfo culture)
    {
        return new MySqlBulkCopy(discoveredTable, connection,culture);
    }

    public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection,IManagedTransaction transaction = null)
    {
        var toReturn = new Dictionary<string,DiscoveredRelationship>();

        const string sql = @"SELECT DISTINCT
u.CONSTRAINT_NAME,
u.TABLE_SCHEMA,
u.TABLE_NAME,
u.COLUMN_NAME,
u.REFERENCED_TABLE_SCHEMA,
u.REFERENCED_TABLE_NAME,
u.REFERENCED_COLUMN_NAME,
c.DELETE_RULE
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE u
INNER JOIN
    INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS c ON c.CONSTRAINT_NAME = u.CONSTRAINT_NAME
WHERE
  u.REFERENCED_TABLE_SCHEMA = @db AND
  u.REFERENCED_TABLE_NAME = @tbl";

        using (var cmd = new MySqlCommand(sql, (MySqlConnection) connection,(MySqlTransaction) transaction?.Transaction))
        {
            var p = new MySqlParameter("@db", MySqlDbType.String)
            {
                Value = table.Database.GetRuntimeName()
            };
            cmd.Parameters.Add(p);

            p = new MySqlParameter("@tbl", MySqlDbType.String)
            {
                Value = table.GetRuntimeName()
            };
            cmd.Parameters.Add(p);

            using var dt = new DataTable();
            var da = table.Database.Server.GetDataAdapter(cmd);
            da.Fill(dt);

            foreach(DataRow r in dt.Rows)
            {
                var fkName = r["CONSTRAINT_NAME"].ToString();

                //could be a 2+ columns foreign key?
                if (!toReturn.TryGetValue(fkName, out var current))
                {
                    var pkDb = r["REFERENCED_TABLE_SCHEMA"].ToString();
                    var pkTableName = r["REFERENCED_TABLE_NAME"].ToString();

                    var fkDb = r["TABLE_SCHEMA"].ToString();
                    var fkTableName =  r["TABLE_NAME"].ToString();

                    var pktable = table.Database.Server.ExpectDatabase(pkDb).ExpectTable(pkTableName);
                    var fktable = table.Database.Server.ExpectDatabase(fkDb).ExpectTable(fkTableName);

                    //https://dev.mysql.com/doc/refman/8.0/en/referential-constraints-table.html
                    var deleteRuleString = r["DELETE_RULE"].ToString();

                    var deleteRule = deleteRuleString switch
                    {
                        "CASCADE" => CascadeRule.Delete,
                        "NO ACTION" => CascadeRule.NoAction,
                        "RESTRICT" => CascadeRule.NoAction,
                        "SET NULL" => CascadeRule.SetNull,
                        "SET DEFAULT" => CascadeRule.SetDefault,
                        _ => CascadeRule.Unknown
                    };

                    current = new DiscoveredRelationship(fkName,pktable,fktable,deleteRule);
                    toReturn.Add(current.Name,current);
                }

                current.AddKeys(r["REFERENCED_COLUMN_NAME"].ToString(), r["COLUMN_NAME"].ToString(), transaction);
            }
        }
            
        return toReturn.Values.ToArray();
    }

    protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
    {
        var syntax = discoveredTable.GetQuerySyntaxHelper();

        return $"RENAME TABLE {discoveredTable.GetWrappedName()} TO {syntax.EnsureWrapped(newName)};";
    }

    public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX)
    {
        return $"SELECT * FROM {table.GetFullyQualifiedName()} LIMIT {topX}";
    }


    public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
    {
        throw new NotImplementedException();
    }
}