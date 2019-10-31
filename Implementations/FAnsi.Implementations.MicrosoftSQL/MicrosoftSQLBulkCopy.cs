using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using FAnsi.Connections;
using FAnsi.Discovery;

namespace FAnsi.Implementations.MicrosoftSQL
{
    public class MicrosoftSQLBulkCopy : BulkCopy
    {
        private readonly SqlBulkCopy _bulkcopy;

        public MicrosoftSQLBulkCopy(DiscoveredTable targetTable, IManagedConnection connection,CultureInfo culture): base(targetTable, connection,culture)
        {
            _bulkcopy = new SqlBulkCopy((SqlConnection)connection.Connection, SqlBulkCopyOptions.KeepIdentity, (SqlTransaction)connection.Transaction)
            {
                BulkCopyTimeout = 50000,
                DestinationTableName = targetTable.GetFullyQualifiedName()
            };
        }

        public override int UploadImpl(DataTable dt)
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

            ConvertStringTypesToHardTypes(dt);

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
                {
                    if(BcpColIdToString(insert,e as SqlException, out string result1, out _))
                        throw new Exception(string.Format(SR.MicrosoftSQLBulkCopy_BulkInsertWithBetterErrorMessages_Failed_to_bulk_insert__0_, result1),e);  //but we can still give him a better message than "bcp colid 1 was bad"!
                }
                else
                {
                    Exception better;
                    try
                    {
                        //we can attempt line by line insert to find the bad row
                        better = AttemptLineByLineInsert(e,insert,dt,serverForLineByLineInvestigation);
                    }
                    catch (Exception exception)
                    {
                        throw new AggregateException(SR.MicrosoftSQLBulkCopy_BulkInsertWithBetterErrorMessages_Failed_to_bulk_insert_batch__line_by_line_investigation_also_failed___InnerException_0__is_the_original_Exception__InnerException_1__is_the_line_by_line_failure,e, exception);
                    }

                    throw better;
                }
                    

                throw;
                
            }
        }

        /// <summary>
        /// Creates a new transaction and does one line at a time bulk insertions of the <paramref name="insert"/> to determine which line (and value)
        /// is causing the problem.  Transaction is always rolled back.
        /// 
        /// </summary>
        /// <param name="e"></param>
        /// <param name="insert"></param>
        /// <param name="dt"></param>
        /// <param name="serverForLineByLineInvestigation"></param>
        /// <returns></returns>
        private Exception AttemptLineByLineInsert(Exception e, SqlBulkCopy insert, DataTable dt, DiscoveredServer serverForLineByLineInvestigation)
        {
            int line = 1;
            string firstPass = ExceptionToListOfInnerMessages(e, true);
            firstPass = firstPass.Replace(Environment.NewLine, Environment.NewLine + "\t");
            firstPass = Environment.NewLine + SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_First_Pass_Exception_ + Environment.NewLine + firstPass;
            
            //have to use a new object because current one could have a broken transaction associated with it
            using (var con = (SqlConnection)serverForLineByLineInvestigation.GetConnection())
            {
                con.Open();
                SqlTransaction investigationTransaction = con.BeginTransaction("Investigate BulkCopyFailure");
                using (SqlBulkCopy investigationOneLineAtATime = new SqlBulkCopy(con,SqlBulkCopyOptions.KeepIdentity,investigationTransaction)
                {
                    DestinationTableName = insert.DestinationTableName
                })
                {
                   
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
                            if (BcpColIdToString(investigationOneLineAtATime,exception as SqlException,out string result, out SqlBulkCopyColumnMapping badMapping))
                            {
                                if (dt.Columns.Contains(badMapping.SourceColumn))
                                {
                                    var sourceValue = dr[badMapping.SourceColumn];
                                    var destColumn = base.TargetTableColumns.SingleOrDefault(c =>c.GetRuntimeName().Equals(badMapping.DestinationColumn));

                                    if(destColumn != null)
                                        return new FileLoadException(
                                            string.Format(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_BulkInsert_failed_on_data_row__0__the_complaint_was_about_source_column____1____which_had_value____2____destination_data_type_was____3____4__5_, line, badMapping.SourceColumn, sourceValue, destColumn.DataType, Environment.NewLine, result), exception);
                                }

                                return new Exception(string.Format(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_BulkInsert_failed_on_data_row__0___1_, line, result), e);
                            }
                        
                            return  new FileLoadException(
                                string.Format(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_Second_Pass_Exception__Failed_to_load_data_row__0__the_following_values_were_rejected_by_the_database___1__2__3_, line, Environment.NewLine, string.Join(Environment.NewLine,dr.ItemArray), firstPass),
                                exception);
                        }

                    //it worked... how!?
                    investigationTransaction.Rollback();
                    con.Close();
                }
                
                return new Exception(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_Second_Pass_Exception__Bulk_insert_failed_but_when_we_tried_to_repeat_it_a_line_at_a_time_it_worked + firstPass , e);
            }
        }

        /// <summary>
        /// Inspects exception message <paramref name="ex"/> for references to bcp client colid and displays the user recognizable name of the column.
        /// </summary>
        /// <param name="insert"></param>
        /// <param name="ex">The Exception you caught.  If null method returns false and output variables are null.</param>
        /// <param name="newMessage"></param>
        /// <param name="badMapping"></param>
        /// <returns></returns>
        private bool BcpColIdToString(SqlBulkCopy insert, SqlException ex, out string newMessage, out SqlBulkCopyColumnMapping badMapping)
        {
            if (ex == null)
            {
                newMessage = null;
                badMapping = null;
                return false;
            }

            Regex columnLevelComplaint = new Regex("bcp client for colid (\\d+)");
            Match match = columnLevelComplaint.Match(ex.Message);
            
            if (match.Success)
            {
                //it counts from 1 not 0.  Also it isn't an index into insert.ColumnMappings.  It's an index into a private field!
                int columnItHates = Convert.ToInt32(match.Groups[1].Value) -1;

                try
                {
                    FieldInfo fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings", BindingFlags.NonPublic | BindingFlags.Instance);
                    var sortedColumns = fi.GetValue(insert);
                    var items = (Object[])sortedColumns.GetType().GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(sortedColumns);

                    FieldInfo itemdata = items[columnItHates].GetType().GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                    var metadata = itemdata.GetValue(items[columnItHates]);
                
                    string destinationColumn = (string)metadata.GetType().GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                
                    var length = metadata.GetType().GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);

                    badMapping = insert.ColumnMappings.Cast<SqlBulkCopyColumnMapping>()
                        .SingleOrDefault(m => string.Equals(m.DestinationColumn , destinationColumn, StringComparison.CurrentCultureIgnoreCase));
                
                    newMessage = ex.Message.Insert(match.Index + match.Length,
                        $"(Source Column <<{badMapping?.SourceColumn??"unknown"}>> Dest Column <<{destinationColumn}>> which has MaxLength of {length})");
                
                    return true;
                }
                catch (Exception)
                {
                    //private fields in SqlBulkCopy have changed name?
                    newMessage = ex.Message;
                    badMapping = null;
                    return false;
                }
            }
            
            newMessage = ex.Message;
            badMapping = null;
            return false;
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