using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using FAnsi.Connections;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;

namespace FAnsi.Discovery
{
    /// <summary>
    /// Contains all the DatabaseType specific implementation logic required by DiscoveredTable.
    /// </summary>
    public interface IDiscoveredTableHelper
    {
        /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetTopXSql"]'/>
        /// <param name="table">The table to fetch records from</param>
        string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX);

        DiscoveredColumn[] DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database);

        IDiscoveredColumnHelper GetColumnHelper();
        
        void DropTable(DbConnection connection, DiscoveredTable tableToDrop);
        void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop);
        void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop);

        void AddColumn(DatabaseOperationArgs args,DiscoveredTable table, string name, string dataType, bool allowNulls);

        int GetRowCount(DbConnection connection, IHasFullyQualifiedNameToo table, DbTransaction dbTransaction = null);

        DiscoveredParameter[] DiscoverTableValuedFunctionParameters(DbConnection connection, DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction transaction);

        IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection,CultureInfo culture);
        
        void TruncateTable(DiscoveredTable discoveredTable);
        void MakeDistinct(DiscoveredTable discoveredTable, int timeoutInSeconds);

        /// <inheritdoc cref="DiscoveredTable.ScriptTableCreation"/>
        string ScriptTableCreation(DiscoveredTable constraints, bool dropPrimaryKeys, bool dropNullability, bool convertIdentityToInt, DiscoveredTable toCreateTable = null);
        bool IsEmpty(DbConnection connection, DiscoveredTable discoveredTable, DbTransaction transaction);
        void RenameTable(DiscoveredTable discoveredTable, string newName, IManagedConnection connection);
        void CreatePrimaryKey(DatabaseOperationArgs args, DiscoveredTable columns, DiscoveredColumn[] discoverColumns);
        int ExecuteInsertReturningIdentity(DiscoveredTable discoveredTable, DbCommand cmd, IManagedTransaction transaction=null);
        DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable discoveredTable,DbConnection connection, IManagedTransaction transaction = null);
        void FillDataTableWithTopX(DiscoveredTable table, int topX, DataTable dt, DbConnection connection, DbTransaction transaction = null);
        
        
        /// <summary>
        /// Creates a new primary key relationship in a foreign key table that points to a primary key table (which must have a primary key)
        /// </summary>
        /// <param name="foreignKeyPairs">
        /// Columns to join up.
        /// Key is the foreign key column (and the table the constraint will be put on).
        /// Value is the primary key table column (which the constraint reference points to)
        /// </param>
        /// <param name="cascadeDeletes"></param>
        /// <param name="constraintName">The name to give the foreign key constraint created, if null then a default name will be picked e.g. FK_Tbl1_Tbl2</param>
        /// <returns></returns>
        DiscoveredRelationship AddForeignKey(DatabaseOperationArgs args, Dictionary<DiscoveredColumn, DiscoveredColumn> foreignKeyPairs, bool cascadeDeletes,string constraintName =null);
    }
}
