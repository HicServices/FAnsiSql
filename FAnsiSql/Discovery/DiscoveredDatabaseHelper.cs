using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.TableCreation;
using FAnsi.Extensions;
using FAnsi.Implementation;
using FAnsi.Naming;
using TypeGuesser;

namespace FAnsi.Discovery
{
    /// <summary>
    /// DBMS specific implementation of all functionality that relates to interacting with existing databases (dropping databases, creating tables, finding stored proceedures etc).  For 
    /// database creation see <see cref="DiscoveredServerHelper"/>
    /// </summary>
    public abstract class DiscoveredDatabaseHelper:IDiscoveredDatabaseHelper
    {
        public abstract IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection,
            string database, bool includeViews, DbTransaction transaction = null);

        public abstract IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
            DbConnection connection, string database, DbTransaction transaction = null);

        public abstract DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database);
        public abstract IDiscoveredTableHelper GetTableHelper();
        public abstract void DropDatabase(DiscoveredDatabase database);
        public abstract Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database);

        public DiscoveredTable CreateTable(CreateTableArgs args)
        {
            var typeDictionary = new Dictionary<string, Guesser>(StringComparer.CurrentCultureIgnoreCase);

            List<DatabaseColumnRequest> columns = new List<DatabaseColumnRequest>();
            List<DatabaseColumnRequest> customRequests = args.ExplicitColumnDefinitions != null
                ? args.ExplicitColumnDefinitions.ToList()
                : new List<DatabaseColumnRequest>();

            if(args.DataTable != null)
            {

                ThrowIfObjectColumns(args.DataTable);

                //If we have a data table from which to create the table from
                foreach (DataColumn column in args.DataTable.Columns)
                {
                    //do we have an explicit overriding column definition?
                    DatabaseColumnRequest overriding = customRequests.SingleOrDefault(c => c.ColumnName.Equals(column.ColumnName,StringComparison.CurrentCultureIgnoreCase));

                    //yes
                    if (overriding != null)
                    {
                        columns.Add(overriding);
                        customRequests.Remove(overriding);

                        //Type requested is a proper FAnsi type (e.g. string, at least 5 long)
                        var request = overriding.TypeRequested;
                        
                        if(request == null)
                            if(!string.IsNullOrWhiteSpace(overriding.ExplicitDbType))
                            {
                                //Type is for an explicit SQL Type e.g. varchar(5)

                                //Translate the sql type to a FAnsi type definition 
                                var tt = args.Database.Server.GetQuerySyntaxHelper().TypeTranslater;

                                request = tt.GetDataTypeRequestForSQLDBType(overriding.ExplicitDbType);
                                
                            }
                            else
                                throw new Exception(string.Format(FAnsiStrings.DiscoveredDatabaseHelper_CreateTable_DatabaseColumnRequestMustHaveEitherTypeRequestedOrExplicitDbType, column));

                        var guesser = GetGuesser(request);
                        CopySettings(guesser, args);
                        typeDictionary.Add(overriding.ColumnName, guesser);
                    }
                    else
                    {
                        //no, work out the column definition using a guesser
                        Guesser guesser = GetGuesser(column);
                        guesser.Culture = args.Culture;
                        
                        CopySettings(guesser,args);

                        guesser.AdjustToCompensateForValues(column);
                        
                        //if DoNotRetype is set on the column adjust the requested CSharpType to be the original type
                        if (column.GetDoNotReType())
                            guesser.Guess.CSharpType = column.DataType;
                        
                        typeDictionary.Add(column.ColumnName,guesser);

                        columns.Add(new DatabaseColumnRequest(column.ColumnName, guesser.Guess, column.AllowDBNull) { IsPrimaryKey = args.DataTable.PrimaryKey.Contains(column)});
                    }
                }
            }
            else
            {
                //If no DataTable is provided just use the explicitly requested columns
                columns = customRequests;
            }

            if(args.Adjuster != null)
                args.Adjuster.AdjustColumns(columns);

            //Get the table creation SQL
            string bodySql = GetCreateTableSql(args.Database, args.TableName, columns.ToArray(), args.ForeignKeyPairs, args.CascadeDelete, args.Schema);

            //connect to the server and send it
            var server = args.Database.Server;

            using (var con = server.GetConnection())
            {
                con.Open();

                ExecuteBatchNonQuery(bodySql, con);
            }

            //Get reference to the newly created table
            var tbl = args.Database.ExpectTable(args.TableName, args.Schema);

            //unless we are being asked to create it empty then upload the DataTable to it
            if(args.DataTable != null && !args.CreateEmpty)
                tbl.BeginBulkInsert().Upload(args.DataTable);

            args.OnTableCreated(typeDictionary);

            return tbl;
        }

        private void CopySettings(Guesser guesser, CreateTableArgs args)
        {
            //cannot change the instance so have to copy across the values.  If this gets new properties that's a problem
            //See tests GuessSettings_CopyProperties
            guesser.Settings.CharCanBeBoolean = args.GuessSettings.CharCanBeBoolean;
            guesser.Settings.ExplicitDateFormats = args.GuessSettings.ExplicitDateFormats;
        }

        /// <summary>
        /// Throws an <see cref="NotSupportedException"/> if the <paramref name="dt"/> contains <see cref="DataColumn"/> with
        /// the <see cref="DataColumn.DataType"/> of <see cref="System.Object"/>
        /// </summary>
        /// <param name="dt"></param>
        public void ThrowIfObjectColumns(DataTable dt)
        {
            var objCol = dt.Columns.Cast<DataColumn>().FirstOrDefault(c => c.DataType == typeof(object));
            
            if(objCol != null)
                throw new NotSupportedException(
                    string.Format(
                        FAnsiStrings.DataTable_Column__0__was_of_DataType__1___this_is_not_allowed___Use_String_for_untyped_data,
                        objCol.ColumnName,
                        objCol.DataType
                        ));
        }

        /// <inheritdoc/>
        public abstract void CreateSchema(DiscoveredDatabase discoveredDatabase, string name);

        protected virtual Guesser GetGuesser(DataColumn column)
        {
            return new Guesser();
        }

        protected virtual Guesser GetGuesser(DatabaseTypeRequest request)
        {
            return new Guesser(request);
        }

        public virtual string GetCreateTableSql(DiscoveredDatabase database, string tableName, DatabaseColumnRequest[] columns, Dictionary<DatabaseColumnRequest, DiscoveredColumn> foreignKeyPairs, bool cascadeDelete, string schema)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(FAnsiStrings.DiscoveredDatabaseHelper_GetCreateTableSql_Table_name_cannot_be_null, "tableName");

            StringBuilder bodySql = new StringBuilder();

            var server = database.Server;
            var syntaxHelper = server.GetQuerySyntaxHelper();

            syntaxHelper.ValidateTableName(tableName);

            foreach (DatabaseColumnRequest c in columns)
                syntaxHelper.ValidateColumnName(c.ColumnName);

            //the name sans brackets (hopefully they didn't pass any brackets)
            tableName = syntaxHelper.GetRuntimeName(tableName);

            //the name uflly specified e.g. [db]..[tbl] or `db`.`tbl` - See Test HorribleColumnNames
            var fullyQualifiedName = syntaxHelper.EnsureFullyQualified(database.GetRuntimeName(), schema, tableName);

            bodySql.AppendLine("CREATE TABLE " + fullyQualifiedName + "(" );

            foreach (var col in columns)
            {
                var datatype = col.GetSQLDbType(syntaxHelper.TypeTranslater);
                
                //add the column name and accompanying datatype
                bodySql.AppendLine(GetCreateTableSqlLineForColumn(col, datatype, syntaxHelper) + ",");
            }

            var pks = columns.Where(c => c.IsPrimaryKey).ToArray();
            if (pks.Any())
                bodySql.Append(GetPrimaryKeyDeclarationSql(tableName, pks,syntaxHelper));
            
            if (foreignKeyPairs != null)
            {
                bodySql.AppendLine();
                bodySql.AppendLine(GetForeignKeyConstraintSql(tableName, syntaxHelper,
                    foreignKeyPairs.ToDictionary(k => (IHasRuntimeName) k.Key, v => v.Value), cascadeDelete, null));
            }

            var toReturn = bodySql.ToString().TrimEnd('\r', '\n', ',');
            
            toReturn += ")" + Environment.NewLine;

            return toReturn;
        }

        /// <summary>
        /// Return the line that represents the given <paramref name="col"/> for slotting into a CREATE statement SQL e.g. "description varchar(20)"
        /// </summary>
        /// <param name="col"></param>
        /// <param name="datatype"></param>
        /// <param name="syntaxHelper"></param>
        /// <returns></returns>
        protected virtual string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype, IQuerySyntaxHelper syntaxHelper)
        {
            return string.Format("{0} {1} {2} {3} {4} {5}",
            syntaxHelper.EnsureWrapped(col.ColumnName),
            datatype,
            col.Default != MandatoryScalarFunctions.None ? "default " + syntaxHelper.GetScalarFunctionSql(col.Default) : "",
            string.IsNullOrWhiteSpace(col.Collation) ? "" : "COLLATE " + col.Collation,
            col.AllowNulls && !col.IsPrimaryKey ? " NULL" : " NOT NULL",
            col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : ""
            );
        }

        public virtual string GetForeignKeyConstraintSql(string foreignTable, IQuerySyntaxHelper syntaxHelper,
            Dictionary<IHasRuntimeName, DiscoveredColumn> foreignKeyPairs, bool cascadeDelete, string constraintName)
        {
            var primaryKeyTable = foreignKeyPairs.Values.Select(v => v.Table).Distinct().Single();

            if (constraintName == null)
                constraintName = GetForeignKeyConstraintNameFor(foreignTable, primaryKeyTable.GetRuntimeName());

            //@"    CONSTRAINT FK_PersonOrder FOREIGN KEY (PersonID) REFERENCES Persons(PersonID) on delete cascade";
            return string.Format(
                @"CONSTRAINT {0} FOREIGN KEY ({1})
REFERENCES {2}({3}) {4}",
                constraintName,
                string.Join(",",foreignKeyPairs.Keys.Select(k=>syntaxHelper.EnsureWrapped(k.GetRuntimeName()))),
                primaryKeyTable.GetFullyQualifiedName(),
                string.Join(",",foreignKeyPairs.Values.Select(v=>syntaxHelper.EnsureWrapped(v.GetRuntimeName()))),
                cascadeDelete ? " on delete cascade": ""
            );
        }
        public string GetForeignKeyConstraintNameFor(DiscoveredTable foreignTable, DiscoveredTable primaryTable)
        {
            return GetForeignKeyConstraintNameFor(foreignTable.GetRuntimeName(), primaryTable.GetRuntimeName());
        }

        protected virtual string GetForeignKeyConstraintNameFor(string foreignTable, string primaryTable)
        {
            return MakeSensibleConstraintName("FK_", foreignTable + "_" + primaryTable);
        }

        public abstract DirectoryInfo Detach(DiscoveredDatabase database);

        public abstract void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName);
        
        protected virtual string GetPrimaryKeyDeclarationSql(string tableName, DatabaseColumnRequest[] pks, IQuerySyntaxHelper syntaxHelper)
        {
            var constraintName = MakeSensibleConstraintName("PK_", tableName);

            return string.Format(" CONSTRAINT {0} PRIMARY KEY ({1})", constraintName, string.Join(",", pks.Select(c => syntaxHelper.EnsureWrapped(c.ColumnName)))) + "," + Environment.NewLine;
        }

        private string MakeSensibleConstraintName(string prefix, string tableName)
        {
            var constraintName = QuerySyntaxHelper.MakeHeaderNameSensible(tableName);

            if (string.IsNullOrWhiteSpace(constraintName))
            {
                Random r = new Random();
                constraintName = "Constraint" + r.Next(10000);
            }

            return prefix + constraintName;
        }
        
        public void ExecuteBatchNonQuery(string sql, DbConnection conn, DbTransaction transaction = null, int timeout = 30)
        {
            Dictionary<int, Stopwatch> whoCares;
            ExecuteBatchNonQuery(sql, conn, transaction, out whoCares, timeout);
        }

        /// <summary>
        /// Executes the given SQL against the database + sends GO delimited statements as separate batches
        /// </summary>
        /// <param name="sql">Collection of SQL queries which can be separated by the use of "GO" on a line (works for all DBMS)</param>
        /// <param name="conn"></param>
        /// <param name="transaction"></param>
        /// <param name="performanceFigures">Line number the batch started at and the time it took to complete it</param>
        /// <param name="timeout">Timeout in seconds to run each batch in the <paramref name="sql"/></param>
        public void ExecuteBatchNonQuery(string sql, DbConnection conn, DbTransaction transaction, out Dictionary<int, Stopwatch> performanceFigures, int timeout = 30)
        {
            performanceFigures = new Dictionary<int, Stopwatch>();

            StringBuilder sqlBatch = new StringBuilder();

            var helper = ImplementationManager.GetImplementation(conn).GetServerHelper();

            using (DbCommand cmd = helper.GetCommand(string.Empty, conn, transaction))
            {
                bool hadToOpen = false;
            
                if (conn.State != ConnectionState.Open)
                {

                    conn.Open();
                    hadToOpen = true;
                }

                int lineNumber = 1;

                sql += "\nGO";   // make sure last batch is executed.
                try
                {
                    foreach (string line in sql.Split(new[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        lineNumber++;

                        if (line.Trim().Equals("GO",StringComparison.CurrentCultureIgnoreCase))
                        {
                            var executeSql = sqlBatch.ToString();
                            if (string.IsNullOrWhiteSpace(executeSql))
                                continue;

                            if (!performanceFigures.ContainsKey(lineNumber))
                                performanceFigures.Add(lineNumber, new Stopwatch());
                            performanceFigures[lineNumber].Start();

                            cmd.CommandText = executeSql;
                            cmd.CommandTimeout = timeout;
                            cmd.ExecuteNonQuery();

                            performanceFigures[lineNumber].Stop();
                            sqlBatch.Clear();
                        }
                        else
                        {
                            sqlBatch.AppendLine(line);
                        }
                    }
                }
                finally
                {
                    if (hadToOpen)
                        conn.Close();
                }
            }
        }
    }
}