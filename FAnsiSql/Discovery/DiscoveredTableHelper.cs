using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;

namespace FAnsi.Discovery
{
    public abstract class DiscoveredTableHelper :IDiscoveredTableHelper
    {
        public abstract string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX);

        public abstract DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database);
        
        public abstract IDiscoveredColumnHelper GetColumnHelper();
        public abstract void DropTable(DbConnection connection, DiscoveredTable tableToDrop);
        public abstract void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop);
        public abstract void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop);

        public virtual void AddColumn(DiscoveredTable table, DbConnection connection, string name, string dataType, bool allowNulls, int timeoutInSeconds)
        {
            var cmd = table.Database.Server.GetCommand("ALTER TABLE " + table.GetFullyQualifiedName() + " ADD " + name + " " + dataType + " " + (allowNulls ? "NULL" : "NOT NULL"), connection);
            cmd.CommandTimeout = timeoutInSeconds;
            cmd.ExecuteNonQuery();
        }

        public abstract int GetRowCount(DbConnection connection, IHasFullyQualifiedNameToo table, DbTransaction dbTransaction = null);

        public abstract DiscoveredParameter[] DiscoverTableValuedFunctionParameters(DbConnection connection, DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction);

        public abstract IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection);

        public virtual void TruncateTable(DiscoveredTable discoveredTable)
        {
            var server = discoveredTable.Database.Server;
            using (var con = server.GetConnection())
            {
                con.Open();
                server.GetCommand("TRUNCATE TABLE " + discoveredTable.GetFullyQualifiedName(), con).ExecuteNonQuery();
            }
        }

        /// <inheritdoc/>
        public string ScriptTableCreation(DiscoveredTable table, bool dropPrimaryKeys, bool dropNullability, bool convertIdentityToInt, DiscoveredTable toCreateTable = null)
        {
            List<DatabaseColumnRequest> columns = new List<DatabaseColumnRequest>();

            foreach (DiscoveredColumn c in table.DiscoverColumns())
            {
                string sqlType = c.DataType.SQLType;

                if (c.IsAutoIncrement && convertIdentityToInt)
                    sqlType = "int";

                bool isToDifferentDatabaseType = toCreateTable != null && toCreateTable.Database.Server.DatabaseType != table.Database.Server.DatabaseType;


                //translate types
                if (isToDifferentDatabaseType)
                {
                    var fromtt = table.Database.Server.GetQuerySyntaxHelper().TypeTranslater;
                    var tott = toCreateTable.Database.Server.GetQuerySyntaxHelper().TypeTranslater;

                    sqlType = fromtt.TranslateSQLDBType(c.DataType.SQLType, tott);
                }

                var colRequest = new DatabaseColumnRequest(c.GetRuntimeName(),sqlType , c.AllowNulls || dropNullability);
                colRequest.IsPrimaryKey = c.IsPrimaryKey && !dropPrimaryKeys;
                
                colRequest.IsAutoIncrement = c.IsAutoIncrement && !convertIdentityToInt;
                colRequest.AllowNulls = colRequest.AllowNulls && !colRequest.IsAutoIncrement;

                //if there is a collation
                if (!string.IsNullOrWhiteSpace(c.Collation))
                {
                    //if the script is to be run on a database of the same type
                    if (toCreateTable == null || !isToDifferentDatabaseType)
                    {
                        //then specify that the column should use the live collation
                        colRequest.Collation = c.Collation;
                    }
                }

                columns.Add(colRequest);
            }

            var destinationTable = toCreateTable ?? table;

            string schema = toCreateTable != null ? toCreateTable.Schema : table.Schema;

            return table.Database.Helper.GetCreateTableSql(destinationTable.Database, destinationTable.GetRuntimeName(), columns.ToArray(), null, false, schema);
        }

        public virtual bool IsEmpty(DbConnection connection, DiscoveredTable discoveredTable, DbTransaction transaction)
        {
            return GetRowCount(connection, discoveredTable, transaction) == 0;
        }

        public virtual void RenameTable(DiscoveredTable discoveredTable, string newName, IManagedConnection connection)
        {
            DbCommand cmd = discoveredTable.Database.Server.Helper.GetCommand(GetRenameTableSql(discoveredTable, newName), connection.Connection, connection.Transaction);
            cmd.ExecuteNonQuery();
        }

        public virtual void CreatePrimaryKey(DiscoveredTable table, DiscoveredColumn[] discoverColumns, IManagedConnection connection, int timeoutInSeconds = 0)
        {
            try{

                string sql = string.Format("ALTER TABLE {0} ADD PRIMARY KEY ({1})",
                    table.GetFullyQualifiedName(),
                    string.Join(",", discoverColumns.Select(c => c.GetRuntimeName()))
                    );

                DbCommand cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
                cmd.CommandTimeout = timeoutInSeconds;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to create primary key on table " + table + " using columns (" + string.Join(",", discoverColumns.Select(c => c.GetRuntimeName())) + ")", e);
            }
        }

        public virtual int ExecuteInsertReturningIdentity(DiscoveredTable discoveredTable, DbCommand cmd, IManagedTransaction transaction=null)
        {
            cmd.CommandText += ";SELECT @@IDENTITY";

            var result = cmd.ExecuteScalar();

            if (result == DBNull.Value || result == null)
                return 0;

            return Convert.ToInt32(result);
        }

        public abstract DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table,DbConnection connection, IManagedTransaction transaction = null);

        public virtual void FillDataTableWithTopX(DiscoveredTable table, int topX, DataTable dt, DbConnection connection, DbTransaction transaction = null)
        {
            string sql = GetTopXSqlForTable(table, topX);

            var da = table.Database.Server.GetDataAdapter(sql, connection);
            da.Fill(dt);
        }

        protected abstract string GetRenameTableSql(DiscoveredTable discoveredTable, string newName);

        public virtual void MakeDistinct(DiscoveredTable discoveredTable, int timeoutInSeconds)
        {
            var server = discoveredTable.Database.Server;

            //note to future developers, this method has horrible side effects e.g. column defaults might be recalculated, foreign key CASCADE Deletes might happen
            //to other tables we can help the user not make such mistakes with this check.
            if(discoveredTable.DiscoverColumns().Any(c => c.IsPrimaryKey))
                throw new NotSupportedException("Table "+discoveredTable+" has primary keys, why are you calling MakeDistinct on it!");

            var tableName = discoveredTable.GetFullyQualifiedName();
            var tempTable = discoveredTable.Database.ExpectTable(discoveredTable.GetRuntimeName() + "_DistinctingTemp").GetFullyQualifiedName();

            using (var con = server.BeginNewTransactedConnection())
            {
                try
                {
                    var cmdDistinct = server.GetCommand(string.Format("CREATE TABLE {1} AS SELECT distinct * FROM {0}", tableName, tempTable), con);
                    cmdDistinct.CommandTimeout = timeoutInSeconds;
                    cmdDistinct.ExecuteNonQuery();

                    var cmdTruncate = server.GetCommand(string.Format("DELETE FROM {0}", tableName), con);
                    cmdTruncate.CommandTimeout = timeoutInSeconds;
                    cmdTruncate.ExecuteNonQuery();

                    var cmdBack = server.GetCommand(string.Format("INSERT INTO {0} (SELECT * FROM {1})", tableName, tempTable), con);
                    cmdBack.CommandTimeout = timeoutInSeconds;
                    cmdBack.ExecuteNonQuery();

                    var cmdDropDistinctTable = server.GetCommand(string.Format("DROP TABLE {0}", tempTable), con);
                    cmdDropDistinctTable.CommandTimeout = timeoutInSeconds;
                    cmdDropDistinctTable.ExecuteNonQuery();

                    con.ManagedTransaction.CommitAndCloseConnection();
                }
                catch (Exception)
                {
                    con.ManagedTransaction.AbandonAndCloseConnection();
                    throw;
                }
            }
        }

        public virtual bool RequiresLength(string columnType)
        {
            columnType = columnType.ToLower();

            switch (columnType)
            {
                case "binary": return true;
                case "bit": return false;
                case "char": return true;
                case "image": return true;
                case "nchar": return true;
                case "nvarchar": return true;
                case "varbinary": return true;
                case "varchar": return true;
                case "numeric": return true;

                default: return false;
            }
        }

        public virtual bool HasPrecisionAndScale(string columnType)
        {
            columnType = columnType.ToLower();

            switch (columnType)
            {
                case "decimal": return true;
                case "numeric": return true;
                default: return false;
            }
        }
    }
}