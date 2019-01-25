﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using FAnsi;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;

namespace FAnsi.Implementations.MicrosoftSQL
{
    public class MicrosoftSQLTableHelper : DiscoveredTableHelper
    {
        public override DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
        {
            DbCommand cmd = discoveredTable.GetCommand("use [" + database + @"];
SELECT  
sys.columns.name AS COLUMN_NAME,
 sys.types.name AS TYPE_NAME,
  sys.columns.collation_name AS COLLATION_NAME,
   sys.columns.max_length as LENGTH,
   sys.columns.scale as SCALE,
    sys.columns.is_identity,
    sys.columns.is_nullable,
   sys.columns.precision as PRECISION,
sys.columns.collation_name
from sys.columns 
join 
sys.types on sys.columns.user_type_id = sys.types.user_type_id
where object_id =OBJECT_ID('" + GetObjectName(discoveredTable) + "')", connection.Connection, connection.Transaction);

            List<DiscoveredColumn> toReturn = new List<DiscoveredColumn>();


            using (var r = cmd.ExecuteReader())
                while (r.Read())
                {
                    bool isNullable = Convert.ToBoolean(r["is_nullable"]);

                    //if it is a table valued function prefix the column name with the table valued function name
                    string columnName = discoveredTable is DiscoveredTableValuedFunction
                        ? discoveredTable.GetRuntimeName() + "." + r["COLUMN_NAME"]
                        : r["COLUMN_NAME"].ToString();

                    var toAdd = new DiscoveredColumn(discoveredTable, columnName, isNullable);
                    toAdd.IsAutoIncrement = Convert.ToBoolean(r["is_identity"]);

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

        /// <summary>
        /// Returns the table name suitable for being passed into OBJECT_ID including schema if any
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        private string GetObjectName(DiscoveredTable table)
        {
            var objectName = table.GetRuntimeName();

            if (table.Schema != null)
                return table.Schema + "." + objectName;

            return objectName;
        }

        public override IDiscoveredColumnHelper GetColumnHelper()
        {
            return new MicrosoftSQLColumnHelper();
        }

        public override void DropTable(DbConnection connection, DiscoveredTable tableToDrop)
        {
            
            SqlCommand cmd;

            switch (tableToDrop.TableType)
            {
                case TableType.View:
                    if (connection.Database != tableToDrop.Database.GetRuntimeName())
                        connection.ChangeDatabase(tableToDrop.GetRuntimeName());

                    if(!connection.Database.ToLower().Equals(tableToDrop.Database.GetRuntimeName().ToLower()))
                        throw new NotSupportedException("Cannot drop view "+tableToDrop +" because it exists in database "+ tableToDrop.Database.GetRuntimeName() +" while the current current database connection is pointed at database:" + connection.Database + " (use .ChangeDatabase on the connection first) - SQL Server does not support cross database view dropping");

                    cmd = new SqlCommand("DROP VIEW " + tableToDrop.GetRuntimeName(), (SqlConnection)connection);
                    break;
                case TableType.Table:
                    cmd = new SqlCommand("DROP TABLE " + tableToDrop.GetFullyQualifiedName(), (SqlConnection)connection);
                    break;
                case TableType.TableValuedFunction :
                    DropFunction(connection,(DiscoveredTableValuedFunction) tableToDrop);
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            cmd.ExecuteNonQuery();
        }

        public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
        {
            SqlCommand cmd = new SqlCommand("DROP FUNCTION " + functionToDrop.GetRuntimeName(), (SqlConnection)connection);
            cmd.ExecuteNonQuery();
        }

        public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
        {
            SqlCommand cmd = new SqlCommand("ALTER TABLE " + columnToDrop.Table.GetFullyQualifiedName() + " DROP column " + columnToDrop.GetRuntimeName(), (SqlConnection)connection);
            cmd.ExecuteNonQuery();
        }

        public override int GetRowCount(DbConnection connection, IHasFullyQualifiedNameToo table, DbTransaction dbTransaction = null)
        {
                    SqlCommand cmdCount = new SqlCommand(@"/*Do not lock anything, and do not get held up by any locks.*/
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
 
-- Quickly get row counts.
declare @rowcount int = (SELECT distinct max(p.rows) AS [Row Count]
FROM sys.partitions p
INNER JOIN sys.indexes i ON p.object_id = i.object_id
                         AND p.index_id = i.index_id
WHERE OBJECT_NAME(p.object_id) = @tableName)

-- if we could not get it quickly then it is probably a view or something so have to return the slow count
if @rowcount is null 
	set @rowcount = (select count(*) from "+table.GetFullyQualifiedName()+@")

select @rowcount", (SqlConnection) connection);

                    cmdCount.Transaction = dbTransaction as SqlTransaction;
                    cmdCount.Parameters.Add(new SqlParameter("@tableName",SqlDbType.VarChar));
                    cmdCount.Parameters["@tableName"].Value = table.GetRuntimeName();

                    return Convert.ToInt32(cmdCount.ExecuteScalar());
        }
        
        public override DiscoveredParameter[] DiscoverTableValuedFunctionParameters(DbConnection connection,DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction)
        {
            string query =
                @"select 
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
where object_id = OBJECT_ID('" + GetObjectName(discoveredTableValuedFunction) + "')";

            DbCommand cmd = discoveredTableValuedFunction.GetCommand(query, connection);
            cmd.Transaction = transaction;

            List<DiscoveredParameter> toReturn = new List<DiscoveredParameter>();

            using (var r = cmd.ExecuteReader())
            {
                while (r.Read())
                {
                    DiscoveredParameter toAdd = new DiscoveredParameter(r["name"].ToString());
                    toAdd.DataType = new DiscoveredDataType(r, GetSQLType_FromSpColumnsResult(r),null);
                    toReturn.Add(toAdd);
                }
            }
            
            return toReturn.ToArray();
        }

        public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable,IManagedConnection connection)
        {
            return new MicrosoftSQLBulkCopy(discoveredTable,connection);
        }

        public override void CreatePrimaryKey(DiscoveredTable table, DiscoveredColumn[] discoverColumns, IManagedConnection connection, int timeoutInSeconds = 0)
        {
            try
            {
                var columnHelper = GetColumnHelper();
                foreach (var col in discoverColumns.Where(dc => dc.AllowNulls))
                {
                    var alterSql = columnHelper.GetAlterColumnToSql(col, col.DataType.SQLType, false);
                    var alterCmd = table.GetCommand(alterSql, connection.Connection, connection.Transaction);
                    alterCmd.CommandTimeout = timeoutInSeconds;
                    alterCmd.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                throw new Exception("Failed to create primary key on table " + table + " using columns (" + string.Join(",", discoverColumns.Select(c => c.GetRuntimeName())) + ")", e);
            }

            base.CreatePrimaryKey(table, discoverColumns, connection, timeoutInSeconds);
        }

        public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table,DbConnection connection, IManagedTransaction transaction = null)
        {
            string sql = "exec sp_fkeys @pktable_name = @table, @pktable_qualifier=@database, @pktable_owner=@schema";

            DbCommand cmd = table.GetCommand(sql, connection);
            if(transaction != null)
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
            

            Dictionary<string,DiscoveredRelationship> toReturn = new Dictionary<string,DiscoveredRelationship>();

            using (var r = cmd.ExecuteReader())
            {
                while (r.Read())
                {
                    var fkName = r["FK_NAME"].ToString();
                    
                    DiscoveredRelationship current;

                    //could be a 2+ columns foreign key?
                    if (toReturn.ContainsKey(fkName))
                    {
                        current = toReturn[fkName];
                    }
                    else
                    {
                        var pkdb = r["PKTABLE_QUALIFIER"].ToString();
                        var pkschema = r["PKTABLE_OWNER"].ToString();
                        var pktableName = r["PKTABLE_NAME"].ToString();

                        var pktable = table.Database.Server.ExpectDatabase(pkdb).ExpectTable(pktableName, pkschema);

                        var fkdb = r["FKTABLE_QUALIFIER"].ToString();
                        var fkschema = r["FKTABLE_OWNER"].ToString();
                        var fktableName = r["FKTABLE_NAME"].ToString();

                        var fktable = table.Database.Server.ExpectDatabase(fkdb).ExpectTable(fktableName, fkschema);

                        var deleteRuleInt = Convert.ToInt32(r["DELETE_RULE"]);
                        CascadeRule deleteRule = CascadeRule.Unknown;
                        

                        if(deleteRuleInt == 0)
                            deleteRule = CascadeRule.Delete;
                        else if(deleteRuleInt == 1)
                            deleteRule = CascadeRule.NoAction;
                        else if (deleteRuleInt == 2)
                            deleteRule = CascadeRule.SetNull;
                        else if (deleteRuleInt == 3)
                            deleteRule = CascadeRule.SetDefault;

                        
                        /*
https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-fkeys-transact-sql?view=sql-server-2017
                         
0=CASCADE changes to foreign key.
1=NO ACTION changes if foreign key is present.
2 = set null
3 = set default*/

                        current = new DiscoveredRelationship(fkName,pktable,fktable,deleteRule);
                        toReturn.Add(current.Name,current);
                    }


                    current.AddKeys(r["PKCOLUMN_NAME"].ToString(), r["FKCOLUMN_NAME"].ToString(),transaction);
                }
            }

            return toReturn.Values.ToArray();

        }

        protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
        {
            string oldName = "["+discoveredTable.GetRuntimeName() +"]";

            if (!string.IsNullOrWhiteSpace(discoveredTable.Schema))
                oldName = "[" + discoveredTable.Schema + "]." + oldName;

            return string.Format("exec sp_rename '{0}', '{1}'", oldName, newName);
        }

        public override void MakeDistinct(DiscoveredTable discoveredTable, int timeoutInSeconds)
        {
            string sql = 
            @"DELETE f
            FROM (
            SELECT	ROW_NUMBER() OVER (PARTITION BY {0} ORDER BY {0}) AS RowNum
            FROM {1}
            
            ) as f
            where RowNum > 1";
            
            string columnList = string.Join(",",discoveredTable.DiscoverColumns().Select(c=>c.GetRuntimeName()));

            string sqlToExecute = string.Format(sql,columnList,discoveredTable.GetFullyQualifiedName());

            var server = discoveredTable.Database.Server;

            using (var con = server.GetConnection())
            {
                con.Open();
                var cmd = server.GetCommand(sqlToExecute, con);
                cmd.CommandTimeout = timeoutInSeconds;
                cmd.ExecuteNonQuery();
            }
        }


        public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX)
        {
            return "SELECT TOP " + topX + " * FROM " + table.GetFullyQualifiedName();
        }
        
        private string GetSQLType_FromSpColumnsResult(DbDataReader r)
        {
            string columnType = r["TYPE_NAME"] as string;
            string lengthQualifier = "";
            
            if (HasPrecisionAndScale(columnType))
                lengthQualifier = "(" + r["PRECISION"] + "," + r["SCALE"] + ")";
            else
                if (RequiresLength(columnType))
                {
                    lengthQualifier = "(" + AdjustForUnicodeAndNegativeOne(columnType,Convert.ToInt32(r["LENGTH"])) + ")";
                }

            if (columnType == "text")
                return "varchar(max)";

            return columnType + lengthQualifier;
        }

        private object AdjustForUnicodeAndNegativeOne(string columnType, int length)
        {
            if (length == -1)
                return "max";

            if (columnType.Contains("nvarchar") || columnType.Contains("nchar") || columnType.Contains("ntext"))
                return length/2;

            return length;
        }


        private string[] ListPrimaryKeys(IManagedConnection con, DiscoveredTable table)
        {
            List<string> toReturn = new List<string>();

            string query = String.Format(@"SELECT i.name AS IndexName, 
OBJECT_NAME(ic.OBJECT_ID) AS TableName, 
COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName, 
c.is_identity
FROM sys.indexes AS i 
INNER JOIN sys.index_columns AS ic 
INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id 
ON i.OBJECT_ID = ic.OBJECT_ID 
AND i.index_id = ic.index_id 
WHERE (i.is_primary_key = 1) AND ic.OBJECT_ID = OBJECT_ID('{0}')
ORDER BY OBJECT_NAME(ic.OBJECT_ID), ic.key_ordinal", GetObjectName(table));

            DbCommand cmd = table.GetCommand(query, con.Connection);
            cmd.Transaction = con.Transaction;

            using(DbDataReader r = cmd.ExecuteReader())
            {

                while (r.Read())
                    toReturn.Add((string) r["ColumnName"]);

                r.Close();
            }
            return toReturn.ToArray();
        }
    }

}