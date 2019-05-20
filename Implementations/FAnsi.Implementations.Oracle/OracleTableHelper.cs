using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle
{
    public class OracleTableHelper : DiscoveredTableHelper
    {

        public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX)
        {
            return "SELECT * FROM " + table.GetFullyQualifiedName() + " WHERE ROWNUM <= " + topX;
        }

        public override DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
        {
            var server = discoveredTable.Database.Server;

            List<DiscoveredColumn> columns = new List<DiscoveredColumn>();
            var tableName = discoveredTable.GetRuntimeName();

            DbCommand cmd = server.Helper.GetCommand(@"SELECT *
FROM   all_tab_cols
WHERE  table_name = :table_name AND owner =:owner AND HIDDEN_COLUMN <> 'YES'
", connection.Connection);
                cmd.Transaction = connection.Transaction;

                DbParameter p = new OracleParameter("table_name", OracleDbType.Varchar2);
                p.Value = tableName;
                cmd.Parameters.Add(p);

                DbParameter p2 = new OracleParameter("owner", OracleDbType.Varchar2);
                p2.Value = database;
                cmd.Parameters.Add(p2);

                using (var r = cmd.ExecuteReader())
                {
                    if (!r.HasRows)
                        throw new Exception("Could not find any columns for table " + tableName +
                                            " in database " + database);

                    while (r.Read())
                    {

                        var toAdd = new DiscoveredColumn(discoveredTable, (string)r["COLUMN_NAME"], r["NULLABLE"].ToString() != "N") { Format = r["CHARACTER_SET_NAME"] as string };
                        toAdd.DataType = new DiscoveredDataType(r, GetSQLType_From_all_tab_cols_Result(r), toAdd);
                        columns.Add(toAdd);
                    }

                }

                //get auto increment information
                cmd = new OracleCommand("select table_name,column_name from ALL_TAB_IDENTITY_COLS WHERE table_name = :table_name AND owner =:owner", (OracleConnection)connection.Connection);
                cmd.Transaction = connection.Transaction;

                p = new OracleParameter("table_name", OracleDbType.Varchar2);
                p.Value = tableName;
                cmd.Parameters.Add(p);

                p2 = new OracleParameter("owner", OracleDbType.Varchar2);
                p2.Value = database;
                cmd.Parameters.Add(p2);

                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                    {
                        var colName = r["column_name"].ToString();
                        var match = columns.Single(c => c.GetRuntimeName().Equals(colName, StringComparison.CurrentCultureIgnoreCase));
                        match.IsAutoIncrement = true;
                    }
                }


                //get primary key information 
                cmd = new OracleCommand(@"SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
FROM all_constraints cons, all_cons_columns cols
WHERE cols.table_name = :table_name AND cols.owner = :owner
AND cons.constraint_type = 'P'
AND cons.constraint_name = cols.constraint_name
AND cons.owner = cols.owner
ORDER BY cols.table_name, cols.position", (OracleConnection) connection.Connection);
                cmd.Transaction = connection.Transaction;


                p = new OracleParameter("table_name",OracleDbType.Varchar2);
                p.Value = tableName;
                cmd.Parameters.Add(p);


                p2 = new OracleParameter("owner", OracleDbType.Varchar2);
                p2.Value = database;
                cmd.Parameters.Add(p2);

                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                        columns.Single(c => c.GetRuntimeName().Equals(r["COLUMN_NAME"])).IsPrimaryKey = true;//mark all primary keys as primary
                }

                return columns.ToArray();
        }


        public override IDiscoveredColumnHelper GetColumnHelper()
        {
            return new OracleColumnHelper();
        }

        public override void DropTable(DbConnection connection, DiscoveredTable table)
        {
            var cmd = new OracleCommand("DROP TABLE " +table.GetFullyQualifiedName(), (OracleConnection)connection);
            cmd.ExecuteNonQuery();
        }

        public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
        {
            var cmd = new OracleCommand("ALTER TABLE " + columnToDrop.Table.GetFullyQualifiedName() + "  DROP COLUMN " + columnToDrop.GetRuntimeName(), (OracleConnection)connection);
            cmd.ExecuteNonQuery();
        }

        public override int GetRowCount(DbConnection connection, IHasFullyQualifiedNameToo table, DbTransaction dbTransaction = null)
        {
            var cmd = new OracleCommand("select count(*) from " + table.GetFullyQualifiedName(), (OracleConnection) connection);
            cmd.Transaction = dbTransaction as OracleTransaction;
            return Convert.ToInt32(cmd.ExecuteScalar());
        }
        
        private string GetBasicTypeFromOracleType(DbDataReader r)
        {
            int? precision = null;
            int? scale = null;
            int? data_length = null; //in bytes

            if (r["DATA_SCALE"] != DBNull.Value)
                scale = Convert.ToInt32(r["DATA_SCALE"]);
            if (r["DATA_PRECISION"] != DBNull.Value)
                precision = Convert.ToInt32(r["DATA_PRECISION"]);
            if(r["DATA_LENGTH"] != DBNull.Value)
                data_length = Convert.ToInt32(r["DATA_LENGTH"]);

            switch (r["DATA_TYPE"] as string)
            {
                //All the ways that you can use the number keyword https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
                case "NUMBER":
                    if (scale == 0 && precision == null)
                        return "int";
                    else if (precision != null && scale != null)
                        return "decimal";
                    else
                    {
                        if (data_length == null)
                            throw new Exception(
                                string.Format(
                                    "Found Oracle NUMBER datatype with scale {0} and precision {1}, did not know what datatype to use to represent it",
                                    scale != null ? scale.ToString() : "DBNull.Value",
                                    precision != null ? precision.ToString() : "DBNull.Value"));
                        else
                            return "double";
                    }
                case "FLOAT":
                    return "double";
                default:
                    return r["DATA_TYPE"].ToString().ToLower();
            }
        }

        private string GetSQLType_From_all_tab_cols_Result(DbDataReader r)
        {
            string columnType = GetBasicTypeFromOracleType(r);

            string lengthQualifier = "";
            
            if (HasPrecisionAndScale(columnType))
                lengthQualifier = "(" + r["DATA_PRECISION"] + "," + r["DATA_SCALE"] + ")";
            else
                if (RequiresLength(columnType))
                    lengthQualifier = "(" + r["DATA_LENGTH"] + ")";

            return columnType + lengthQualifier;
        }

        public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
        {
            throw new NotImplementedException();
        }
        
        public override DiscoveredParameter[] DiscoverTableValuedFunctionParameters(DbConnection connection,
            DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection)
        {
            return new OracleBulkCopy(discoveredTable,connection);
        }

        public override int ExecuteInsertReturningIdentity(DiscoveredTable discoveredTable, DbCommand cmd, IManagedTransaction transaction = null)
        {
            var autoIncrement = discoveredTable.DiscoverColumns(transaction).SingleOrDefault(c => c.IsAutoIncrement);

            if (autoIncrement == null)
                return Convert.ToInt32(cmd.ExecuteScalar());

            var p = discoveredTable.Database.Server.Helper.GetParameter("identityOut");
            p.Direction = ParameterDirection.Output;
            p.DbType = DbType.Int32;

            cmd.Parameters.Add(p);

            cmd.CommandText += " RETURNING " + autoIncrement + " INTO :identityOut;";

            cmd.CommandText = "BEGIN " +Environment.NewLine + cmd.CommandText + Environment.NewLine + "COMMIT;" + Environment.NewLine + "END;";

            cmd.ExecuteNonQuery();
            

            return Convert.ToInt32(p.Value);
        }

        public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection,
            IManagedTransaction transaction = null)
        {
            string sql = @"
SELECT a.table_name
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
AND  UPPER(c_pk.table_name) =  UPPER(:TableName)";


            var cmd = new OracleCommand(sql, (OracleConnection)connection);

            var p = new OracleParameter(":DatabaseName", OracleDbType.Varchar2);
            p.Value = table.Database.GetRuntimeName();
            cmd.Parameters.Add(p);

            p = new OracleParameter(":TableName", OracleDbType.Varchar2);
            p.Value = table.GetRuntimeName();
            cmd.Parameters.Add(p);

            Dictionary<string, DiscoveredRelationship> toReturn = new Dictionary<string, DiscoveredRelationship>();

            using (var r = cmd.ExecuteReader())
            {
                while (r.Read())
                {
                    var fkName = r["constraint_name"].ToString();

                    DiscoveredRelationship current;

                    //could be a 2+ columns foreign key?
                    if (toReturn.ContainsKey(fkName))
                    {
                        current = toReturn[fkName];
                    }
                    else
                    {

                        var pkDb = r["r_owner"].ToString();
                        var pkTableName = r["r_table_name"].ToString();

                        var fkDb = r["owner"].ToString();
                        var fkTableName = r["table_name"].ToString();

                        var pktable = table.Database.Server.ExpectDatabase(pkDb).ExpectTable(pkTableName);
                        var fktable = table.Database.Server.ExpectDatabase(fkDb).ExpectTable(fkTableName);

                        //https://dev.mysql.com/doc/refman/8.0/en/referential-constraints-table.html
                        var deleteRuleString = r["delete_rule"].ToString();

                        CascadeRule deleteRule = CascadeRule.Unknown;

                        if (deleteRuleString == "CASCADE")
                            deleteRule = CascadeRule.Delete;
                        else if (deleteRuleString == "NO ACTION")
                            deleteRule = CascadeRule.NoAction;
                        else if (deleteRuleString == "RESTRICT")
                            deleteRule = CascadeRule.NoAction;
                        else if (deleteRuleString == "SET NULL")
                            deleteRule = CascadeRule.SetNull;
                        else if (deleteRuleString == "SET DEFAULT")
                            deleteRule = CascadeRule.SetDefault;

                        current = new DiscoveredRelationship(fkName, pktable, fktable, deleteRule);
                        toReturn.Add(current.Name, current);
                    }

                    current.AddKeys(r["r_column_name"].ToString(), r["column_name"].ToString(), transaction);
                }
            }

            return toReturn.Values.ToArray();
        }

        public override void FillDataTableWithTopX(DiscoveredTable table, int topX, DataTable dt, DbConnection connection,DbTransaction transaction = null)
        {
            ((OracleConnection)connection).PurgeStatementCache();

            var cols = table.DiscoverColumns();

            string sql = "SELECT " + string.Join(",", cols.Select(c => c.GetFullyQualifiedName()).ToArray()) + " FROM " + table.GetFullyQualifiedName() + " WHERE ROWNUM <= " + topX;

            var da = table.Database.Server.GetDataAdapter(sql, connection);
            da.Fill(dt);
        }


        protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
        {
            return string.Format(@"alter table {0} rename to {1};", discoveredTable.GetRuntimeName(),newName);
        }

        public override bool RequiresLength(string columnType)
        {
            return base.RequiresLength(columnType) || columnType.Equals("varchar2", StringComparison.CurrentCultureIgnoreCase);
        }
    }
}