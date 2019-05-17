using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery.Constraints;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Naming;

namespace FAnsi.Discovery
{
    /// <summary>
    /// Cross database type reference to a Table (or view) in a Database.  Use TableType to determine whether it is a view or a table.  Allows you to check
    /// existence, drop, add columns, get row counts etc.
    /// </summary>
    public class DiscoveredTable :IHasFullyQualifiedNameToo, IMightNotExist, IHasQuerySyntaxHelper, IEquatable<DiscoveredTable>
    {
        private string _table;

        /// <summary>
        /// Helper for generating queries compatible with the DBMS the table exists in (e.g. TOP X, column qualifiers, what the parameter symbol is etc).
        /// </summary>
        protected IQuerySyntaxHelper QuerySyntaxHelper;

        /// <summary>
        /// The database on which the table exists
        /// </summary>
        public DiscoveredDatabase Database { get; private set; }

        /// <summary>
        /// Stateless helper class with DBMS specific implementation of the logic required by <see cref="DiscoveredTable"/>.
        /// </summary>
        public IDiscoveredTableHelper Helper { get; set; }

        /// <summary>
        /// <para>Schema of the <see cref="Database"/> the table exists in (or null).  This is NOT the database e.g. in [MyDb].[dbo].[MyTable] the schema is "dbo".</para>
        /// 
        /// <para>Null if not supported by the DBMS (e.g. MySql)</para>
        /// </summary>
        public string Schema { get; private set; }

        /// <summary>
        /// Whether the table referenced is a normal table, view or table valued function (see derrived class <see cref="DiscoveredTableValuedFunction"/>)
        /// </summary>
        public TableType TableType { get; private set; }

        /// <summary>
        /// Internal API constructor intended for Implementation classes, instead use <see cref="DiscoveredDatabase.ExpectTable"/> instead.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="table"></param>
        /// <param name="querySyntaxHelper"></param>
        /// <param name="schema"></param>
        /// <param name="tableType"></param>
        public DiscoveredTable(DiscoveredDatabase database, string table, IQuerySyntaxHelper querySyntaxHelper, string schema = null, TableType tableType = TableType.Table)
        {
            _table = table;
            Helper = database.Helper.GetTableHelper();
            Database = database;
            Schema = schema;
            TableType = tableType;

            QuerySyntaxHelper = querySyntaxHelper;
        }
        
        /// <summary>
        /// <para>Checks that the <see cref="Database"/> exists then lists the tables in the database to confirm this table exists on the server</para>
        /// </summary>
        /// <param name="transaction">Optional - if set the connection to list tables will be sent on the connection on which the current
        /// <paramref name="transaction"/> is open</param>
        /// <returns></returns>
        public virtual bool Exists(IManagedTransaction transaction = null)
        {
            if (!Database.Exists())
                return false;

            return Database.DiscoverTables(true, transaction)
               .Any(t => t.GetRuntimeName().Equals(GetRuntimeName(),StringComparison.InvariantCultureIgnoreCase));
        }

        /// <summary>
        /// Returns the unqualified name of the table e.g. "MyTable"
        /// </summary>
        /// <returns></returns>
        public virtual string GetRuntimeName()
        {
            return QuerySyntaxHelper.GetRuntimeName(_table);
        }

        /// <summary>
        /// Returns the fully qualified (including schema if appropriate) name of the table e.g. [MyDb].dbo.[MyTable] or `MyDb`.`MyTable`
        /// </summary>
        /// <returns></returns>
        public virtual string GetFullyQualifiedName()
        {
            return QuerySyntaxHelper.EnsureFullyQualified(Database.GetRuntimeName(),Schema, GetRuntimeName());
        }

        /// <summary>
        /// Connects to the server and returns a list of columns found in the table as <see cref="DiscoveredColumn"/>.
        /// </summary>
        /// <param name="managedTransaction">Optional - if set the connection to list tables will be sent on the connection on which the current
        /// <paramref name="managedTransaction"/> is open</param>
        /// <returns></returns>
        public virtual DiscoveredColumn[] DiscoverColumns(IManagedTransaction managedTransaction=null)
        {
            using (var connection = Database.Server.GetManagedConnection(managedTransaction))
                return Helper.DiscoverColumns(this, connection, Database.GetRuntimeName());
        }

        /// <summary>
        /// Returns the table name
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return _table;
        }

        /// <summary>
        /// Gets helper for generating queries compatible with the DBMS the table exists in (e.g. TOP X, column qualifiers, what the parameter symbol is etc).
        /// </summary>
        /// <returns></returns>
        public IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return QuerySyntaxHelper;
        }

        /// <summary>
        /// Returns from <see cref="DiscoverColumns"/> the <paramref name="specificColumnName"/> on the server.  This is not not case sensitive.  Requires
        /// connecting to the database. 
        /// </summary>
        /// <param name="specificColumnName">The column you want to find</param>
        /// <param name="transaction">Optional - if set the connection to list tables will be sent on the connection on which the current
        /// <paramref name="transaction"/> is open</param>
        /// <returns></returns>
        public DiscoveredColumn DiscoverColumn(string specificColumnName,IManagedTransaction transaction=null)
        {
            try
            {
                return DiscoverColumns(transaction).Single(c => c.GetRuntimeName().Equals(QuerySyntaxHelper.GetRuntimeName(specificColumnName), StringComparison.CurrentCultureIgnoreCase));
            }
            catch (Exception e)
            {
                throw new Exception("Could not find column called " + specificColumnName + " in table " + this ,e);
            }
        }

        /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetTopXSql"]'/>
        public string GetTopXSql(int topX)
        {
            return Helper.GetTopXSqlForTable(this, topX);
        }


        /// <summary>
        /// Returns up to 2,147,483,647 records from the table as a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="topX">The maximum number of records to return from the table</param>
        /// <param name="enforceTypesAndNullness">True to set <see cref="DataColumn"/> constraints on the <see cref="DataTable"/> returned e.g. AllowDBNull based on the table
        /// schema of the <see cref="DiscoveredTable"/></param>
        /// <param name="transaction">Optional - if set the connection to fetch the data will be sent on the connection on which the current <paramref name="transaction"/> is open</param>
        /// <returns></returns>
        public virtual DataTable GetDataTable(int topX = int.MaxValue,bool enforceTypesAndNullness = true, IManagedTransaction transaction = null)
        {
            var dt = new DataTable();
            
            if (enforceTypesAndNullness)
                foreach (DiscoveredColumn c in DiscoverColumns(transaction))
                {
                    var col = dt.Columns.Add(c.GetRuntimeName());
                    col.AllowDBNull = c.AllowNulls;
                    col.DataType = c.DataType.GetCSharpDataType();
                }

            using(var con = Database.Server.GetManagedConnection(transaction))
                Helper.FillDataTableWithTopX(this,topX,dt,con.Connection,con.Transaction);

            return dt;
        }
        
        /// <summary>
        /// Drops (deletes) the table from the database.  This is irreversible unless you have a database backup. 
        /// </summary>
        public virtual void Drop()
        {
            using(var connection = Database.Server.GetManagedConnection())
            {
                Helper.DropTable(connection.Connection,this);
            }
        }

        /// <summary>
        /// Returns the estimated number of rows in the table.  This may use a short cut e.g. consulting sys.partitions in Sql
        /// Server (https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-partitions-transact-sql?view=sql-server-2017)
        /// </summary>
        /// <param name="transaction">Optional - if set the query will be sent on the connection on which the current <paramref name="transaction"/> is open</param>
        /// <returns></returns>
        public int GetRowCount(IManagedTransaction transaction = null)
        {
            using (IManagedConnection connection = Database.Server.GetManagedConnection(transaction))
                return Helper.GetRowCount(connection.Connection, this, connection.Transaction);
        }
        
        /// <summary>
        /// Returns true if there are no rows in the table
        /// </summary>
        /// <param name="transaction">Optional - if set the query will be sent on the connection on which the current <paramref name="transaction"/> is open</param>
        /// <returns></returns>
        public bool IsEmpty(IManagedTransaction transaction = null)
        {
            using (IManagedConnection connection = Database.Server.GetManagedConnection(transaction))
                return Helper.IsEmpty(connection.Connection, this, connection.Transaction);
        }

        /// <summary>
        /// Creates and runs an ALTER TABLE SQL statement that adds a new column to the table
        /// </summary>
        /// <param name="name">The unqualified name for the new column e.g. "MyCol2"</param>
        /// <param name="type">The data type for the new column</param>
        /// <param name="allowNulls">True to allow null</param>
        /// <param name="timeoutInSeconds">The length of time to wait in seconds before giving up (See <see cref="DbCommand.CommandTimeout"/>)</param>
        public void AddColumn(string name, DatabaseTypeRequest type,bool allowNulls,int timeoutInSeconds)
        {
            AddColumn(name, Database.Server.GetQuerySyntaxHelper().TypeTranslater.GetSQLDBTypeForCSharpType(type), allowNulls, timeoutInSeconds);
        }


        /// <summary>
        /// Creates and runs an ALTER TABLE SQL statement that adds a new column to the table
        /// </summary>
        /// <param name="name">The unqualified name for the new column e.g. "MyCol2"</param>
        /// <param name="databaseType">The proprietary SQL data type for the new column</param>
        /// <param name="allowNulls">True to allow null</param>
        /// <param name="timeoutInSeconds">The length of time to wait in seconds before giving up (See <see cref="DbCommand.CommandTimeout"/>)</param>
        public void AddColumn(string name, string databaseType, bool allowNulls, int timeoutInSeconds)
        {
            using (IManagedConnection connection = Database.Server.GetManagedConnection())
            {
                Helper.AddColumn(this, connection.Connection, name, databaseType, allowNulls, timeoutInSeconds);
            }
        }

        /// <summary>
        /// Creates and runs an ALTER TABLE SQL statement to drop the given column from the table
        /// </summary>
        /// <param name="column">The column to drop</param>
        public void DropColumn(DiscoveredColumn column)
        {
            using (IManagedConnection connection = Database.Server.GetManagedConnection())
            {
                Helper.DropColumn(connection.Connection, column);
            }
        }
        
        /// <summary>
        /// Creates a new object for bulk inserting records into the table.  You should use a using block since <see cref="IBulkCopy"/> is <see cref="IDisposable"/>.
        /// Depending on implementation, records may not be committed to the server until the <see cref="IBulkCopy"/> is disposed.
        /// </summary>
        /// <param name="transaction">Optional - records inserted should form part of the supplied ongoing transaction</param>
        /// <returns></returns>
        public IBulkCopy BeginBulkInsert(IManagedTransaction transaction = null)
        {
            Database.Server.EnableAsync();
            IManagedConnection connection = Database.Server.GetManagedConnection(transaction);
            return Helper.BeginBulkInsert(this, connection);
        }

        /// <summary>
        /// Creates and runs a TRUNCATE TABLE SQL statement to delete all rows from the table.  Depending on DBMS and table constraints this might fail (e.g. if there are
        /// foreign key constraints on the table).
        /// </summary>
        public void Truncate()
        {
            Helper.TruncateTable(this);
        }

        /// <summary>
        /// Deletes all EXACT duplicate rows from the table leaving only unique records.  This is method may not be transaction/threadsafe
        /// </summary>
        /// <param name="timeoutInSeconds">The length of time to allow for the command to complete (See <see cref="DbCommand.CommandTimeout"/>)</param>
        public void MakeDistinct(int timeoutInSeconds=30)
        {
            Helper.MakeDistinct(this,timeoutInSeconds);
        }


        /// <summary>
        /// <para>Scripts the table columns, optionally adjusting for nullability / identity etc.  Optionally translates the SQL to run and create a table in a different
        /// database / database language / table name</para>
        /// 
        /// <para>Does not include foreign key constraints, dependant tables, CHECK constraints etc</para>
        /// </summary>
        /// <param name="dropPrimaryKeys">True if the resulting script should exclude any primary keys</param>
        /// <param name="dropNullability">True if the resulting script should always allow nulls into columns</param>
        /// <param name="convertIdentityToInt">True if the resulting script should replace identity columns with int in the generated SQL</param>
        /// <param name="toCreateTable">Optional, If provided the SQL generated will be adjusted to create the alternate table instead (which could include going cross server type e.g. MySql to Sql Server)
        /// <para>When using this parameter the table must not exist yet, use destinationDiscoveredDatabase.ExpectTable("MyYetToExistTable")</para></param>
        /// <returns></returns>
        public string ScriptTableCreation(bool dropPrimaryKeys, bool dropNullability, bool convertIdentityToInt, DiscoveredTable toCreateTable = null)
        {
            return Helper.ScriptTableCreation(this, dropPrimaryKeys, dropNullability, convertIdentityToInt, toCreateTable);
        }

        /// <summary>
        /// Issues a database command to rename the table on the database server.
        /// </summary>
        /// <param name="newName"></param>
        public void Rename(string newName)
        {
            using (IManagedConnection connection = Database.Server.GetManagedConnection())
            {
                Helper.RenameTable(this,newName,connection);
                _table = newName;
            }
        }

        /// <summary>
        /// Creates a primary key on the table if none exists yet
        /// </summary>
        /// <param name="discoverColumns">Columns that should become part of the primary key</param>
        public void CreatePrimaryKey(params DiscoveredColumn[] discoverColumns)
        {
            CreatePrimaryKey(0, discoverColumns);
        }

        /// <summary>
        /// Creates a primary key on the table if none exists yet
        /// </summary>
        /// <param name="timeoutInSeconds">The number of seconds to wait for the operation to complete</param>
        /// <param name="discoverColumns">Columns that should become part of the primary key</param>
        public void CreatePrimaryKey(int timeoutInSeconds, params DiscoveredColumn[] discoverColumns)
        {
            using (var connection = Database.Server.GetManagedConnection())
                Helper.CreatePrimaryKey(this, discoverColumns, connection, timeoutInSeconds);
        }
        
        /// <summary>
        /// Inserts the values specified into the database table and returns the last autonum identity generated (or 0 if none present)
        /// </summary>
        /// <param name="toInsert"></param>
        /// <returns></returns>
        public int Insert(Dictionary<DiscoveredColumn,object> toInsert, IManagedTransaction transaction = null)
        {
            var syntaxHelper = GetQuerySyntaxHelper();
            var server = Database.Server;
                       
            var _parameterNames = syntaxHelper.GetParameterNamesFor(toInsert.Keys.ToArray(),c=>c.GetRuntimeName());

            using (IManagedConnection connection = Database.Server.GetManagedConnection(transaction))
            {
                string sql = 
                    string.Format("INSERT INTO {0}({1}) VALUES ({2})",
                    GetFullyQualifiedName(),
                    string.Join(",",toInsert.Keys.Select(c=>syntaxHelper.EnsureWrapped(c.GetRuntimeName()))),
                    string.Join(",",toInsert.Keys.Select(c=>_parameterNames[c]))
                    );

                var cmd = server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);

                foreach (KeyValuePair<DiscoveredColumn, object> kvp in toInsert)
                {
                    var parameter = server.Helper.GetParameter(_parameterNames[kvp.Key]);

                    var p = GetQuerySyntaxHelper().GetParameter(parameter, kvp.Key, kvp.Value);
                    cmd.Parameters.Add(p);
                }

                int result = Helper.ExecuteInsertReturningIdentity(this, cmd, connection.ManagedTransaction);

                return result;
            }
        }

        /// <summary>
        /// Overload which will discover the columns by name for you.
        /// </summary>
        /// <param name="toInsert"></param>
        /// <param name="transaction">ongoing transaction this insert should be part of</param>
        /// <returns></returns>
        public int Insert(Dictionary<string, object> toInsert, IManagedTransaction transaction = null)
        {
            var cols = DiscoverColumns(transaction);

            var foundColumns = new Dictionary<DiscoveredColumn, object>();

            foreach (var k in toInsert.Keys)
            {
                var match = cols.SingleOrDefault(c => c.GetRuntimeName().Equals(k, StringComparison.CurrentCultureIgnoreCase));
                if(match == null)
                    throw new Exception("Could not find column called " + k);

                foundColumns.Add(match,toInsert[k]);
            }

            return Insert(foundColumns, transaction);
        }

        /// <summary>
        /// See <see cref="DiscoveredServerHelper.GetCommand"/>
        /// </summary>
        public DbCommand GetCommand(string s, DbConnection con, DbTransaction transaction = null)
        {
            return Database.Server.Helper.GetCommand(s, con, transaction);
        }

        /// <summary>
        /// Returns all foreign keys where this table is the parent table (i.e. the primary key table).
        /// </summary>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public DiscoveredRelationship[] DiscoverRelationships(IManagedTransaction transaction = null)
        {
            using (IManagedConnection connection = Database.Server.GetManagedConnection(transaction))
                return Helper.DiscoverRelationships(this, connection.Connection,transaction);
        }

        /// <summary>
        /// Based on table name, schema, database and TableType
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(DiscoveredTable other)
        {
            if (ReferenceEquals(null, other)) return false;

            return
                string.Equals(_table, other._table, StringComparison.OrdinalIgnoreCase)
                && string.Equals(GetSchemaWithDboForNull(), other.GetSchemaWithDboForNull(), StringComparison.OrdinalIgnoreCase)
                && Equals(Database, other.Database) && TableType == other.TableType;
        }

        private string GetSchemaWithDboForNull()
        {
            //for "dbo, "" and null are all considered the same
            return string.IsNullOrWhiteSpace(Schema) ? "dbo" : Schema;
        }

        /// <summary>
        /// Based on table name, schema, database and TableType
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((DiscoveredTable)obj);
        }

        /// <summary>
        /// Based on table name, schema, database and TableType
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode =  StringComparer.OrdinalIgnoreCase.GetHashCode(GetSchemaWithDboForNull());
                hashCode = (hashCode * 397) ^ (Database != null ? Database.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int)TableType;
                return hashCode;
            }
        }
    }
}