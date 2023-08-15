using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using TypeGuesser;
using TypeGuesser.Deciders;

namespace FAnsi.Discovery;

/// <inheritdoc/>
public abstract class BulkCopy:IBulkCopy
{
    public CultureInfo Culture { get; }

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

    /// <inheritdoc/>
    public DateTimeTypeDecider DateTimeDecider {get; protected set; }

    /// <summary>
    /// Begins a new bulk copy operation in which one or more data tables are uploaded to the <paramref name="targetTable"/>.  The API entrypoint for this is
    /// <see cref="DiscoveredTable.BeginBulkInsert(IManagedTransaction)"/>.
    /// 
    /// </summary>
    /// <param name="targetTable"></param>
    /// <param name="connection"></param>
    /// <param name="culture">For parsing string date expressions etc</param>
    protected BulkCopy(DiscoveredTable targetTable, IManagedConnection connection,CultureInfo culture)
    {
        Culture = culture;
        TargetTable = targetTable;
        Connection = connection;
        InvalidateTableSchema();
        AllowUnmatchedInputColumns = false;
        DateTimeDecider = new DateTimeTypeDecider(culture);
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
    /// Closes the connection and completes the bulk insert operation (including committing the transaction).  If this method is not called
    /// then the records may not be committed.
    /// </summary>
    public virtual void Dispose()
    {
        Connection.Dispose();
    }

    /// <inheritdoc/>
    public virtual int Upload(DataTable dt)
    {
        TargetTable.Database.Helper.ThrowIfObjectColumns(dt);

        ConvertStringTypesToHardTypes(dt);
            
        return UploadImpl(dt);
    }

    public abstract int UploadImpl(DataTable dt);

    /// <summary>
    /// Replaces all string representations for data types that can be problematic/ambiguous (e.g. DateTime or TimeSpan)
    ///  into hard typed objects using appropriate decider e.g. <see cref="DateTimeDecider"/>.
    /// </summary>
    /// <param name="dt"></param>
    protected void ConvertStringTypesToHardTypes(DataTable dt)
    {
        var dict = GetMapping(dt.Columns.Cast<DataColumn>(),out _);

        var factory = new TypeDeciderFactory(Culture);
            
        //These are the problematic Types
        var deciders = factory.Dictionary;
            
        //for each column in the destination
        foreach(var kvp in dict)
        {
            //if the destination column is a problematic type
            var dataType = kvp.Value.DataType.GetCSharpDataType();
            if (!deciders.TryGetValue(dataType, out var decider)) continue;
            //if it's already not a string then that's fine (hopefully its a legit Type e.g. DateTime!)
            if(kvp.Key.DataType != typeof(string))
                continue;

            //create a new column hard typed to DateTime
            var newColumn = dt.Columns.Add($"{kvp.Key.ColumnName}_{Guid.NewGuid()}",dataType);

            //if it's a DateTime decider then guess DateTime culture based on values in the table
            if(decider is DateTimeTypeDecider)
            {
                //also use this one in case the user has set up explicit stuff on it e.g. Culture/Settings
                decider = DateTimeDecider;
                DateTimeDecider.GuessDateFormat(dt.Rows.Cast<DataRow>().Take(500).Select(r=>r[kvp.Key] as string));
            }


            foreach(DataRow dr in dt.Rows)
            {
                try
                {
                    //parse the value
                    dr[newColumn] = decider.Parse(dr[kvp.Key] as string)??DBNull.Value;

                }
                catch(Exception ex)
                {
                    throw new Exception($"Failed to parse value '{dr[kvp.Key]}' in column '{kvp.Key}'",ex);
                }
            }

            //if the DataColumn is part of the Primary Key of the DataTable (in memory)
            //then we need to update the primary key to include the new column not the old one
            if(dt.PrimaryKey != null && dt.PrimaryKey.Contains(kvp.Key))
                dt.PrimaryKey = dt.PrimaryKey.Except(new []{kvp.Key }).Union(new []{newColumn }).ToArray();

            var oldOrdinal  = kvp.Key.Ordinal;

            //drop the original column
            dt.Columns.Remove(kvp.Key);

            //rename the hard typed column to match the old column name
            newColumn.ColumnName = kvp.Key.ColumnName;
            if(oldOrdinal != -1)
                newColumn.SetOrdinal(oldOrdinal);
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
        var mapping = new Dictionary<DataColumn, DiscoveredColumn>();

        foreach (var colInSource in inputColumns)
        {
            var match = TargetTableColumns.SingleOrDefault(c => c.GetRuntimeName().Equals(colInSource.ColumnName, StringComparison.CurrentCultureIgnoreCase));

            if (match == null)
            {
                if (!AllowUnmatchedInputColumns)
                    throw new ColumnMappingException(string.Format(FAnsiStrings.BulkCopy_ColumnNotInDestinationTable, colInSource.ColumnName, TargetTable));

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
        return GetMapping(inputColumns, out _);
    }
}