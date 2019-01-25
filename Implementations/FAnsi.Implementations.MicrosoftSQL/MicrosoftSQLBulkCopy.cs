using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using FAnsi;
using FAnsi.Connections;
using FAnsi.Discovery;

namespace FAnsi.Implementations.MicrosoftSQL
{
    public class MicrosoftSQLBulkCopy : BulkCopy
    {
        private SqlBulkCopy _bulkcopy;

        public MicrosoftSQLBulkCopy(DiscoveredTable targetTable, IManagedConnection connection): base(targetTable, connection)
        {
            _bulkcopy = new SqlBulkCopy((SqlConnection)connection.Connection, SqlBulkCopyOptions.KeepIdentity, (SqlTransaction)connection.Transaction);
            _bulkcopy.BulkCopyTimeout = 50000;
            _bulkcopy.DestinationTableName = targetTable.GetFullyQualifiedName();
        }

        public override int Upload(DataTable dt)
        {
            _bulkcopy.BulkCopyTimeout = Timeout;

            _bulkcopy.ColumnMappings.Clear();

            foreach (KeyValuePair<DataColumn, DiscoveredColumn> kvp in GetMapping(dt.Columns.Cast<DataColumn>()))
                _bulkcopy.ColumnMappings.Add(kvp.Key.ColumnName, kvp.Value.GetRuntimeName());
            
            return BulkInsertWithBetterErrorMessages(_bulkcopy, dt, TargetTable.Database.Server);
        }

        public int BulkInsertWithBetterErrorMessages(SqlBulkCopy insert, DataTable dt, DiscoveredServer serverForLineByLineInvestigation)
        {
            int rowsWritten = 0;

            EmptyStringsToNulls(dt);

            InspectDataTableForFloats(dt);

            try
            {
                //send data read to server
                insert.WriteToServer(dt);
                rowsWritten += dt.Rows.Count;

                return rowsWritten;
            }
            catch (Exception e)
            {
                //user does not want to replay the load one line at a time to get more specific error messages
                if (serverForLineByLineInvestigation == null)
                    throw;

                int line = 1;
                string baseException = ExceptionToListOfInnerMessages(e, true);
                baseException = baseException.Replace(Environment.NewLine, Environment.NewLine + "\t");
                baseException = Environment.NewLine + "First Pass Exception:" + Environment.NewLine + baseException;
                baseException += Environment.NewLine + "Second Pass Exception:";

                DiscoveredColumn[] destinationColumnsContainedInMapping;

                try
                {
                    var dest = serverForLineByLineInvestigation.GetCurrentDatabase().ExpectTable(insert.DestinationTableName);
                    destinationColumnsContainedInMapping = dest.DiscoverColumns().Where(c => insert.ColumnMappings.Cast<SqlBulkCopyColumnMapping>().Any(m => m.DestinationColumn == c.GetRuntimeName())).ToArray();
                }
                catch (Exception)
                {
                    throw e; //couldn't even enumerate the destination columns, whatever the original Exception was it must be serious, just rethrow it
                }

                //have to use a new object because current one could have a broken transaction associated with it
                using (var con = (SqlConnection)serverForLineByLineInvestigation.GetConnection())
                {
                    con.Open();
                    SqlTransaction investigationTransaction = con.BeginTransaction("Investigate BulkCopyFailure");
                    SqlBulkCopy investigationOneLineAtATime = new SqlBulkCopy(con, SqlBulkCopyOptions.KeepIdentity, investigationTransaction);
                    investigationOneLineAtATime.DestinationTableName = insert.DestinationTableName;

                    foreach (SqlBulkCopyColumnMapping m in insert.ColumnMappings)
                        investigationOneLineAtATime.ColumnMappings.Add(m);

                    //try a line at a time
                    foreach (DataRow dr in dt.Rows)
                        try
                        {
                            investigationOneLineAtATime.WriteToServer(new[] { dr }); //try one line
                            line++;
                        }
                        catch (Exception exception)
                        {
                            Regex columnLevelComplaint = new Regex("bcp client for colid (\\d+)");

                            Match match = columnLevelComplaint.Match(exception.Message);

                            if (match.Success)
                            {
                                //it counts from 1 because its stupid, hence to get our column index we subtract 1 from whatever column it is complaining about
                                int columnItHates = Convert.ToInt32(match.Groups[1].Value) - 1;

                                if (destinationColumnsContainedInMapping.Length > columnItHates)
                                {
                                    var offendingColumn = destinationColumnsContainedInMapping[columnItHates];

                                    if (dt.Columns.Contains(offendingColumn.GetRuntimeName()))
                                    {
                                        var sourceValue = dr[offendingColumn.GetRuntimeName()];
                                        throw new FileLoadException(baseException + "BulkInsert complained on data row " + line + " the complaint was about column number " + columnItHates + ": <<" + offendingColumn.GetRuntimeName() + ">> which had value <<" + sourceValue + ">> destination data type was <<" + offendingColumn.DataType + ">>", exception);
                                    }
                                }
                            }

                            throw new FileLoadException(
                                baseException + "Failed to load data row " + line + " the following values were rejected by the database: " +
                                Environment.NewLine + dr.ItemArray.Aggregate((o, n) => o + Environment.NewLine + n),
                                exception);
                        }

                    //it worked... how!?
                    investigationTransaction.Rollback();
                    con.Close();

                    throw new Exception(baseException + "Bulk insert failed but when we tried to repeat it a line at a time it worked... very strange", e);
                }

            }
        }
        private static void EmptyStringsToNulls(DataTable dt)
        {
            foreach (var col in dt.Columns.Cast<DataColumn>().Where(c => c.DataType == typeof(string)))
                foreach (DataRow row in dt.Rows)
                {
                    var o = row[col];

                    if (o == DBNull.Value || o == null)
                        continue;

                    if (string.IsNullOrWhiteSpace(o.ToString()))
                        row[col] = DBNull.Value;
                }
        }

        [Pure]
        public string ExceptionToListOfInnerMessages(Exception e, bool includeStackTrace = false)
        {
            string message = e.Message;
            if (includeStackTrace)
                message += Environment.NewLine + e.StackTrace;

            if (e is ReflectionTypeLoadException)
                foreach (Exception loaderException in ((ReflectionTypeLoadException)e).LoaderExceptions)
                    message += Environment.NewLine + ExceptionToListOfInnerMessages(loaderException, includeStackTrace);

            if (e.InnerException != null)
                message += Environment.NewLine + ExceptionToListOfInnerMessages(e.InnerException, includeStackTrace);

            return message;
        }

        private void InspectDataTableForFloats(DataTable dt)
        {
            //are there any float or float? columns
            DataColumn[] floatColumns = dt.Columns.Cast<DataColumn>()
                .Where(
                    c => c.DataType == typeof(float) || c.DataType == typeof(float?)).ToArray();

            if (floatColumns.Any())
                throw new NotSupportedException("Found float column(s) in data table, SQLServer does not support floats in bulk insert, instead you should use doubles otherwise you will end up with the value 0.85 turning into :0.850000023841858 in your database.  Float column(s) were:" + string.Join(",", floatColumns.Select(c => c.ColumnName).ToArray()));

            //are there any object columns
            var objectColumns = dt.Columns.Cast<DataColumn>().Where(c => c.DataType == typeof(object)).ToArray();

            //do any of the object columns have floats or float? in them?
            for (int i = 0; i < Math.Min(100, dt.Rows.Count); i++)
                foreach (DataColumn c in objectColumns)
                {
                    object value = dt.Rows[i][c.ColumnName];

                    Type underlyingType = value.GetType();

                    if (underlyingType == typeof(float) || underlyingType == typeof(float?))
                        throw new NotSupportedException("Found float value " + value + " in data table, SQLServer does not support floats in bulk insert, instead you should use doubles otherwise you will end up with the value 0.85 turning into :0.850000023841858 in your database");
                }
        }
    }
}