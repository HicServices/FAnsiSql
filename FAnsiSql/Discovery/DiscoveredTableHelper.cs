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

namespace FAnsi.Discovery
{
    /// <summary>
    /// DBMS specific implementation of all functionality that relates to interacting with existing tables (altering, dropping, truncating etc).  For table creation
    /// see <see cref="DiscoveredDatabaseHelper"/>.
    /// </summary>
    public abstract class DiscoveredTableHelper :IDiscoveredTableHelper
    {
        public abstract string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX);

        public abstract DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database);
        
        public abstract IDiscoveredColumnHelper GetColumnHelper();
        public virtual void DropTable(DbConnection connection, DiscoveredTable tableToDrop)
        {
            string sql;
            switch (tableToDrop.TableType)
            {
                case TableType.Table:
                    sql = "DROP TABLE {0}"; break;
                case TableType.View:
                    sql = "DROP VIEW {0}"; break;
                case TableType.TableValuedFunction:
                    throw new NotSupportedException();
                default:
                    throw new ArgumentOutOfRangeException("Unknown TableType");
            }
            
            var cmd = tableToDrop.GetCommand(string.Format(sql,tableToDrop.GetFullyQualifiedName()),connection);
            cmd.ExecuteNonQuery();
        }

        public abstract void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop);
        public abstract void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop);

        public virtual void AddColumn(DatabaseOperationArgs args,DiscoveredTable table, string name, string dataType, bool allowNulls)
        {
            using (var con = args.GetManagedConnection(table))
            {
                var cmd = table.Database.Server.GetCommand("ALTER TABLE " + table.GetFullyQualifiedName() + " ADD " + name + " " + dataType + " " + (allowNulls ? "NULL" : "NOT NULL"),con);
                args.ExecuteNonQuery(cmd);
            }
        }

        public virtual int GetRowCount(DatabaseOperationArgs args, DiscoveredTable table)
        {
            using (IManagedConnection connection = args.GetManagedConnection(table))
            {
                var cmd  = table.Database.Server.GetCommand("SELECT count(*) FROM " + table.GetFullyQualifiedName(), connection);
                return Convert.ToInt32(args.ExecuteScalar(cmd));
            }
        }

        public abstract DiscoveredParameter[] DiscoverTableValuedFunctionParameters(DbConnection connection, DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction);

        public abstract IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection,CultureInfo culture);

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

        public virtual bool IsEmpty(DatabaseOperationArgs args, DiscoveredTable discoveredTable)
        {
            return GetRowCount(args, discoveredTable) == 0;
        }

        public virtual void RenameTable(DiscoveredTable discoveredTable, string newName, IManagedConnection connection)
        {
            if(discoveredTable.TableType != TableType.Table)
                throw new NotSupportedException(string.Format(FAnsiStrings.DiscoveredTableHelper_RenameTable_Rename_is_not_supported_for_TableType__0_, discoveredTable.TableType));

            discoveredTable.GetQuerySyntaxHelper().ValidateTableName(newName);

            DbCommand cmd = discoveredTable.Database.Server.Helper.GetCommand(GetRenameTableSql(discoveredTable, newName), connection.Connection, connection.Transaction);
            cmd.ExecuteNonQuery();
        }

        public virtual void CreatePrimaryKey(DatabaseOperationArgs args, DiscoveredTable table, DiscoveredColumn[] discoverColumns)
        {
            using (var connection = args.GetManagedConnection(table))
            {
                try{

                    string sql = string.Format("ALTER TABLE {0} ADD PRIMARY KEY ({1})",
                        table.GetFullyQualifiedName(),
                        string.Join(",", discoverColumns.Select(c => c.GetRuntimeName()))
                    );

                    DbCommand cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);

                    args.ExecuteNonQuery(cmd);
                }
                catch (Exception e)
                {
                    throw new AlterFailedException(string.Format(FAnsiStrings.DiscoveredTableHelper_CreatePrimaryKey_Failed_to_create_primary_key_on_table__0__using_columns___1__, table, string.Join(",", discoverColumns.Select(c => c.GetRuntimeName()))), e);
                }
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

        public virtual void FillDataTableWithTopX(DatabaseOperationArgs args,DiscoveredTable table, int topX, DataTable dt)
        {
            string sql = GetTopXSqlForTable(table, topX);

            using (var con = args.GetManagedConnection(table))
            {
                var cmd = table.Database.Server.GetCommand(sql, con);
                var da = table.Database.Server.GetDataAdapter(cmd);
                args.Fill(da,cmd, dt);
            }
        }

        /// <inheritdoc/>
        public virtual DiscoveredRelationship AddForeignKey(DatabaseOperationArgs args,Dictionary<DiscoveredColumn, DiscoveredColumn> foreignKeyPairs, bool cascadeDeletes, string constraintName = null)
        {
            var foreignTables = foreignKeyPairs.Select(c => c.Key.Table).Distinct().ToArray();
            var primaryTables= foreignKeyPairs.Select(c => c.Value.Table).Distinct().ToArray();

            if(primaryTables.Length != 1 || foreignTables.Length != 1)
                throw new ArgumentException("Primary and foreign keys must all belong to the same table",nameof(foreignKeyPairs));


            var primary = primaryTables[0];
            var foreign = foreignTables[0];

            if (constraintName == null)
                constraintName = primary.Database.Helper.GetForeignKeyConstraintNameFor(foreign, primary);

            string constraintBit = primary.Database.Helper.GetForeignKeyConstraintSql(foreign.GetRuntimeName(), primary.GetQuerySyntaxHelper(),
                foreignKeyPairs
                    .ToDictionary(k=>(IHasRuntimeName)k.Key,v=>v.Value), cascadeDeletes, constraintName);

            string sql = $@"ALTER TABLE {foreign.GetFullyQualifiedName()}
                ADD " + constraintBit;

            using (var con = args.GetManagedConnection(primary))
            {
                try
                {
                    args.ExecuteNonQuery(primary.Database.Server.GetCommand(sql, con));
                }
                catch (Exception e)
                {
                    throw new AlterFailedException("Failed to create relationship using SQL:" + sql,e);
                }
            }

            return primary.DiscoverRelationships(args.TransactionIfAny).Single(
                r =>r.Name.Equals(constraintName,StringComparison.CurrentCultureIgnoreCase)
            );
        }

        protected abstract string GetRenameTableSql(DiscoveredTable discoveredTable, string newName);

        public virtual void MakeDistinct(DatabaseOperationArgs args,DiscoveredTable discoveredTable)
        {
            var server = discoveredTable.Database.Server;

            //if it's got a primary key they it's distinct! job done
            if (discoveredTable.DiscoverColumns().Any(c => c.IsPrimaryKey))
                return;

            var tableName = discoveredTable.GetFullyQualifiedName();
            var tempTable = discoveredTable.Database.ExpectTable(discoveredTable.GetRuntimeName() + "_DistinctingTemp").GetFullyQualifiedName();


            using (var con = args.TransactionIfAny  == null ? 
                server.BeginNewTransactedConnection():  //start a new transaction
                args.GetManagedConnection(server))      //or continue the ongoing transaction
            {
                var cmdDistinct = server.GetCommand(string.Format("CREATE TABLE {1} AS SELECT distinct * FROM {0}", tableName, tempTable), con);
                args.ExecuteNonQuery(cmdDistinct);

                //this is the point of no return so don't cancel after this point
                var cmdTruncate = server.GetCommand(string.Format("DELETE FROM {0}", tableName), con);
                cmdTruncate.CommandTimeout = args.TimeoutInSeconds;
                cmdTruncate.ExecuteNonQuery();

                var cmdBack = server.GetCommand(string.Format("INSERT INTO {0} (SELECT * FROM {1})", tableName, tempTable), con);
                cmdBack.CommandTimeout = args.TimeoutInSeconds;
                cmdBack.ExecuteNonQuery();

                var cmdDropDistinctTable = server.GetCommand(string.Format("DROP TABLE {0}", tempTable), con);
                cmdDropDistinctTable.CommandTimeout = args.TimeoutInSeconds;
                cmdDropDistinctTable.ExecuteNonQuery();

                //if we opened a new transaction we should commit it
                if(args.TransactionIfAny == null)
                    con.ManagedTransaction?.CommitAndCloseConnection();
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