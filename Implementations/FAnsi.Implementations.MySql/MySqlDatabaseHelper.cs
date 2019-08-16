using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using MySql.Data.MySqlClient;

namespace FAnsi.Implementations.MySql
{
    public class MySqlDatabaseHelper : DiscoveredDatabaseHelper
    {
        public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
            DbConnection connection, string database, DbTransaction transaction = null)
        {
            return Enumerable.Empty<DiscoveredTableValuedFunction>();
        }

        public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database)
        {
            throw new NotImplementedException();
        }

        public override IDiscoveredTableHelper GetTableHelper()
        {
            return new MySqlTableHelper();
        }

        public override void DropDatabase(DiscoveredDatabase database)
        {
            using (var con = (MySqlConnection) database.Server.GetConnection())
            {
                con.Open();
                MySqlCommand cmd = new MySqlCommand("DROP DATABASE `" + database.GetRuntimeName() +"`",con);
                cmd.ExecuteNonQuery();
            }
        }

        public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
        {
            var mysqlBuilder = (MySqlConnectionStringBuilder) builder;

            var toReturn = new Dictionary<string, string>();
            toReturn.Add("UserID", mysqlBuilder.UserID);
            toReturn.Add("Server", mysqlBuilder.Server);
            toReturn.Add("Database", mysqlBuilder.Database);

            return toReturn;
        }

        public override DirectoryInfo Detach(DiscoveredDatabase database)
        {
            throw new NotImplementedException();
        }

        public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction transaction = null)
        {
            if (connection.State == ConnectionState.Closed)
                throw new InvalidOperationException("Expected connection to be open");

            List<DiscoveredTable> tables = new List<DiscoveredTable>();

            var cmd = new MySqlCommand("SHOW FULL TABLES in `" + database +"`", (MySqlConnection) connection);
            cmd.Transaction = transaction as MySqlTransaction;

            var r = cmd.ExecuteReader();
            while (r.Read())
            {
                bool isView = (string)r[1] == "VIEW";

                //if we are skipping views
                if(isView && !includeViews)
                    continue;
                
                //skip invalid table names
                if(!querySyntaxHelper.IsValidTableName((string)r[0],out _))
                    continue;

                tables.Add(new DiscoveredTable(parent,(string)r[0],querySyntaxHelper,null,isView ? TableType.View : TableType.Table));//this table fieldname will be something like Tables_in_mydbwhatevernameitis
            }
                
            
            return tables.ToArray();
        }

    }
}