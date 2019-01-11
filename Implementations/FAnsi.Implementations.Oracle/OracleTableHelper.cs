﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
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
            throw new NotImplementedException();
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

            cmd.Parameters.Add(p);

            cmd.CommandText += " RETURNING " + autoIncrement + " INTO :identityOut;";

            cmd.ExecuteNonQuery();
            

            return Convert.ToInt32(p.Value);
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