using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlTableHelper : DiscoveredTableHelper
    {
        public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX)
        {
            return "SELECT * FROM " + table.GetFullyQualifiedName() + " FETCH FIRST "+topX+" ROWS ONLY";
        }

        public override DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
        {
            string sqlColumns = @"SELECT *
                FROM information_schema.columns
            WHERE table_schema = @schemaName
            AND table_name   = @tableName;";

            DbCommand cmd = discoveredTable.GetCommand(sqlColumns, connection.Connection, connection.Transaction);

            var p = cmd.CreateParameter();
            p.ParameterName = "@tableName";
            p.Value = discoveredTable.GetRuntimeName();
            cmd.Parameters.Add(p);
            
            var p2 = cmd.CreateParameter();
            p2.ParameterName = "@schemaName";
            p2.Value = string.IsNullOrWhiteSpace(discoveredTable.Schema) ? PostgreSqlSyntaxHelper.DefaultPostgresSchema : discoveredTable.Schema;
            cmd.Parameters.Add(p2);

            List<DiscoveredColumn> toReturn = new List<DiscoveredColumn>();


            using (var r = cmd.ExecuteReader())
                while (r.Read())
                {
                    bool isNullable = string.Equals(r["is_nullable"] , "YES");

                    //if it is a table valued function prefix the column name with the table valued function name
                    string columnName = discoveredTable is DiscoveredTableValuedFunction
                        ? discoveredTable.GetRuntimeName() + "." + r["column_name"]
                        : r["column_name"].ToString();
                        
                    var toAdd = new DiscoveredColumn(discoveredTable, columnName, isNullable);
                    toAdd.IsAutoIncrement = string.Equals(r["is_identity"],"YES");

                    toAdd.DataType = new DiscoveredDataType(r, GetSQLType_FromSpColumnsResult(r), toAdd);
                    toAdd.Collation = r["collation_name"] as string;
                    toReturn.Add(toAdd);
                }

            if(!toReturn.Any())
                throw new Exception("Could not find any columns in table " + discoveredTable);
            
            //don't bother looking for pks if it is a table valued function
            if (discoveredTable is DiscoveredTableValuedFunction)
                return toReturn.ToArray();
            
            var pks = ListPrimaryKeys(connection, discoveredTable);

            foreach (DiscoveredColumn c in toReturn)
                if (pks.Any(pk=>pk.Equals(c.GetRuntimeName())))
                    c.IsPrimaryKey = true;


            return toReturn.ToArray();
        }

        private string[] ListPrimaryKeys(IManagedConnection con, DiscoveredTable table)
        {
            string query = $@"SELECT               
            pg_attribute.attname, 
            format_type(pg_attribute.atttypid, pg_attribute.atttypmod) 
            FROM pg_index, pg_class, pg_attribute 
            WHERE 
            pg_class.oid = '{table.GetFullyQualifiedName()}'::regclass AND 
                indrelid = pg_class.oid AND  
            pg_attribute.attrelid = pg_class.oid AND 
            pg_attribute.attnum = any(pg_index.indkey)
            AND indisprimary";

            List<string> toReturn = new List<string>();
                
            DbCommand cmd = table.GetCommand(query, con.Connection);
            cmd.Transaction = con.Transaction;

            using(DbDataReader r = cmd.ExecuteReader())
            {
                while (r.Read())
                    toReturn.Add((string) r["attname"]);

                r.Close();
            }
            return toReturn.ToArray();
        }

        private string GetSQLType_FromSpColumnsResult(DbDataReader r)
        {
            string columnType = r["data_type"] as string;
            string lengthQualifier = "";
            
            if (HasPrecisionAndScale(columnType))
                lengthQualifier = "(" + r["numeric_precision"] + "," + r["numeric_scale"] + ")";
            else
            if (r["character_maximum_length"] != DBNull.Value)
            {
                lengthQualifier = "(" + Convert.ToInt32(r["character_maximum_length"]) + ")";
            }

            if (columnType == "text")
                return "varchar(max)";

            return columnType + lengthQualifier;
        }


        public override IDiscoveredColumnHelper GetColumnHelper()
        {
            return new PostgreSqlColumnHelper();
        }

        public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
        {
            throw new NotImplementedException();
        }

        public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
        {
            throw new NotImplementedException();
        }

        public override DiscoveredParameter[] DiscoverTableValuedFunctionParameters(DbConnection connection,
            DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture)
        {
            return new PostgreSqlBulkCopy(discoveredTable, connection,culture);
        }

        public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection,
            IManagedTransaction transaction = null)
        {
            throw new NotImplementedException();
        }

        protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
        {
            throw new NotImplementedException();
        }
    }
}