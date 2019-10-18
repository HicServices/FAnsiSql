using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlDatabaseHelper : DiscoveredDatabaseHelper
    {
        public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection,
            string database, bool includeViews, DbTransaction transaction = null)
        {

            string sqlTables = @"SELECT
                *
                FROM
            pg_catalog.pg_tables
                WHERE
            schemaname != 'pg_catalog'
            AND schemaname != 'information_schema';";

            
            string sqlViews = @"SELECT
                *
                FROM
            pg_catalog.pg_views
                WHERE
            schemaname != 'pg_catalog'
            AND schemaname != 'information_schema';";

            List<DiscoveredTable> tables = new List<DiscoveredTable>();
            
            var cmd = new NpgsqlCommand(sqlTables, (NpgsqlConnection)connection);
            cmd.Transaction = transaction as NpgsqlTransaction;

            using (var r = cmd.ExecuteReader())
                while (r.Read())
                {
                    //its a system table
                    string schema = r["schemaname"] as string;
                    
                    if(querySyntaxHelper.IsValidTableName((string)r["tablename"], out _))
                        tables.Add(new DiscoveredTable(parent, (string)r["tablename"], querySyntaxHelper, schema, TableType.Table));
                }

            if (includeViews)
            {
                cmd = new NpgsqlCommand(sqlViews, (NpgsqlConnection)connection);
                cmd.Transaction = transaction as NpgsqlTransaction;

                using (var r = cmd.ExecuteReader())
                    while (r.Read())
                    {
                        //its a system table
                        string schema = r["schemaname"] as string;
                    
                        if(querySyntaxHelper.IsValidTableName((string)r["tablename"], out _))
                            tables.Add(new DiscoveredTable(parent, (string)r["tablename"], querySyntaxHelper, schema, TableType.View));
                    }
            }
            
            return tables.ToArray();
        }

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
            return new PostgreSqlTableHelper();
        }

        public override void DropDatabase(DiscoveredDatabase database)
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
        {
            throw new NotImplementedException();
        }

        public override DirectoryInfo Detach(DiscoveredDatabase database)
        {
            throw new NotImplementedException();
        }

        public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
        {
            throw new NotImplementedException();
        }

        public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
        {
            using (var con = discoveredDatabase.Server.GetConnection())
            {
                con.Open();

                string sql = $@"create schema if not exists {name}";

                var cmd = discoveredDatabase.Server.GetCommand(sql, con);
                cmd.ExecuteNonQuery();
            }
        }
    }
}