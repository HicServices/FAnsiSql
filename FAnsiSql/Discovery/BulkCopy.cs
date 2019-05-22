using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery.TypeTranslation.TypeDeciders;

namespace FAnsi.Discovery
{
    /// <inheritdoc/>
    public abstract class BulkCopy:IBulkCopy
    {
        /// <summary>
        /// The database connection on which the bulk insert operation is underway
        /// </summary>
        protected readonly IManagedConnection Connection;

        /// <summary>
        /// The target table on the database server to which records are being uploaded
        /// </summary>
        protected readonly DiscoveredTable TargetTable;

        /// <summary>
        /// The cached columns found on the <see cref="TargetTable"/>.  If you alter the table midway through a bulk insert you must
        /// call <see cref="InvalidateTableSchema"/> to refresh this.
        /// </summary>
        protected DiscoveredColumn[] TargetTableColumns;
        
        /// <summary>
        /// When calling GetMapping if there are DataColumns in the input table that you are trying to bulk insert that are not matched
        /// in the destination table then the default behaviour is to throw a KeyNotFoundException.  Set this to false to ignore that
        /// behaviour.  This will result in loosing data from your DataTable.
        /// 
        /// <para>Defaults to false</para>
        /// </summary>
        public bool AllowUnmatchedInputColumns { get; private set; }

        protected DateTimeTypeDecider DateTimeDecider = new DateTimeTypeDecider();

        /// <summary>
        /// Begins a new bulk copy operation in which one or more data tables are uploaded to the <paramref name="targetTable"/>.  The API entrypoint for this is
        /// <see cref="DiscoveredTable.BeginBulkInsert(IManagedTransaction)"/>.
        /// 
        /// </summary>
        /// <param name="targetTable"></param>
        /// <param name="connection"></param>
        protected BulkCopy(DiscoveredTable targetTable, IManagedConnection connection)
        {
            TargetTable = targetTable;
            Connection = connection;
            InvalidateTableSchema();
            AllowUnmatchedInputColumns = false;
        }


        /// <inheritdoc/>
        public virtual int Timeout { get; set; }

        /// <summary>
        /// Updates <see cref="TargetTableColumns"/>.  Call if you are making modifications to the <see cref="TargetTable"/> midway through a bulk insert.
        /// </summary>
        public void InvalidateTableSchema()
        {
            TargetTableColumns = TargetTable.DiscoverColumns(Connection.ManagedTransaction);
        }

        /// <summary>
        /// Closes the connection and completes the bulk insert operation (including comitting the transaction).  If this method is not called
        /// then the records may not be committed.
        /// </summary>
        public virtual void Dispose()
        {
            Connection.Dispose();
        }

        /// <inheritdoc/>
        public virtual int Upload(DataTable dt)
        {
            ConvertStringDatesToDateTime(dt);

            return UploadImpl(dt);
        }

        public abstract int UploadImpl(DataTable dt);

        /// <summary>
        /// Replaces all string representations destined to end up in DateTime or TimeSpan fields in the target database
        /// into hard typed objects (using <see cref="DateTimeDecider"/>)
        /// </summary>
        /// <param name="dt"></param>
        protected void ConvertStringDatesToDateTime(DataTable dt)
        {
            var dict = GetMapping(dt.Columns.Cast<DataColumn>(),out _);
                    
            //for each column in the destination
            foreach(var kvp in dict)
            {
                //if the destination column is a date based column
                var dataType = kvp.Value.DataType.GetCSharpDataType();
                if(dataType == typeof(DateTime) || dataType == typeof(TimeSpan))
                {
                    //if it's already not a string then that's fine (hopefully its a DateTime!)
                    if(kvp.Key.DataType != typeof(string))
                        continue;

                    //create a new column hard typed to DateTime
                    var newColumn = dt.Columns.Add(kvp.Key.ColumnName + "_" + Guid.NewGuid().ToString(),dataType);
                    
                    //guess the DateTime culture based on values in the table
                    DateTimeDecider.GuessDateFormat(dt.Rows.Cast<DataRow>().Take(500).Select(r=>r[kvp.Key] as string));

                    foreach(DataRow dr in dt.Rows)
                    {
                        //parse the value

                        var val = DateTimeDecider.Parse(dr[kvp.Key] as string)??DBNull.Value;
                        if(dataType == typeof(DateTime))
                            dr[newColumn] = val;
                        else
                            dr[newColumn] = val == DBNull.Value? val:((DateTime)val).TimeOfDay;
                    }

                    //drop the original column
                    dt.Columns.Remove(kvp.Key);

                    //rename the hard typed column to match the old column name
                    newColumn.ColumnName = kvp.Key.ColumnName;                    
                }
            }
        }

        /// <summary>
        /// Returns a case insensitive mapping between columns in your DataTable that you are trying to upload and the columns that actually exist in the destination 
        /// table.  
        /// <para>This overload gives you a list of all unmatched destination columns, these should be given null/default automatically by your database API</para>
        /// <para>Throws <exception cref="KeyNotFoundException"> if there are unmatched input columns unless <see cref="AllowUnmatchedInputColumns"/> is true.</exception></para>
        /// </summary>
        /// <param name="inputColumns"></param>
        /// <param name="unmatchedColumnsInDestination"></param>
        /// <returns></returns>
        protected Dictionary<DataColumn, DiscoveredColumn> GetMapping(IEnumerable<DataColumn> inputColumns, out DiscoveredColumn[] unmatchedColumnsInDestination)
        {
            Dictionary<DataColumn, DiscoveredColumn> mapping = new Dictionary<DataColumn, DiscoveredColumn>();

            foreach (DataColumn colInSource in inputColumns)
            {
                var match = TargetTableColumns.SingleOrDefault(c => c.GetRuntimeName().Equals(colInSource.ColumnName, StringComparison.CurrentCultureIgnoreCase));

                if (match == null)
                {
                    if (!AllowUnmatchedInputColumns)
                        throw new KeyNotFoundException("Column " + colInSource.ColumnName + " appears in pipeline but not destination table (" + TargetTable + ")");

                    //user is ignoring the fact there are unmatched items in DataTable!
                }
                else
                    mapping.Add(colInSource, match);
            }

            //unmatched columns in the destination is fine, these usually get populated with the default column values or nulls
            unmatchedColumnsInDestination = TargetTableColumns.Except(mapping.Values).ToArray();

            return mapping;
        }

        /// <summary>
        /// Returns a case insensitive mapping between columns in your DataTable that you are trying to upload and the columns that actually exist in the destination 
        /// table.  
        /// <para>Throws <exception cref="KeyNotFoundException"> if there are unmatched input columns unless <see cref="AllowUnmatchedInputColumns"/> is true.</exception></para>
        /// </summary>
        /// <param name="inputColumns"></param>
        /// <returns></returns>
        protected Dictionary<DataColumn,DiscoveredColumn> GetMapping(IEnumerable<DataColumn> inputColumns)
        {
            DiscoveredColumn[] whoCares;
            return GetMapping(inputColumns, out whoCares);
        }
    }
}