using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Connections;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Exceptions;
using TypeGuesser;

namespace FAnsi.Discovery;

/// <summary>
/// Cross database type reference to a Data Type string (e.g. varchar(30), varbinary(100) etc) of a Column in a Table
/// </summary>
public class DiscoveredDataType
{
    private readonly DiscoveredColumn Column;

    /// <summary>
    /// The proprietary DBMS name for the datatype e.g. varchar2(100) for Oracle, datetime2 for Sql Server etc.
    /// </summary>
    public string SQLType { get; set; }

    /// <summary>
    /// All values read from the database record retrieved when assembling the data type (E.g. the cells of the sys.columns record)
    /// </summary>
    public Dictionary<string, object> ProprietaryDatatype = new();

    /// <summary>
    /// API constructor, instead use <see cref="DiscoveredTable.DiscoverColumns"/> instead.
    /// </summary>
    /// <param name="r">All the values in r will be copied into the Dictionary property of this class called ProprietaryDatatype</param>
    /// <param name="sqlType">Your inferred SQL data type for it e.g. varchar(50)</param>
    /// <param name="column">The column it belongs to, can be null e.g. if your data type belongs to a DiscoveredParameter instead</param>
    public DiscoveredDataType(DbDataReader r, string sqlType, DiscoveredColumn column)
    {
        SQLType = sqlType;
        Column = column;

        for (var i = 0; i < r.FieldCount; i++)
            ProprietaryDatatype.Add(r.GetName(i), r.GetValue(i));
    }

    /// <summary>
    /// <para>Returns the maximum string length supported by the described data type or -1 if it isn't a string</para>
    /// <para>Returns <see cref="int.MaxValue"/> if the string type has no real limit e.g. "text"</para>
    /// </summary>
    /// <returns></returns>
    public int GetLengthIfString()
    {
        return Column.Table.Database.Server.Helper.GetQuerySyntaxHelper().TypeTranslater.GetLengthIfString(SQLType);
    }

    /// <summary>
    /// <para>Returns the Scale/Precision of the data type.  Only applies to decimal(x,y) types not basic types e.g. int.</para>
    /// 
    /// <para>Returns null if the datatype is not floating point</para>
    /// </summary>
    /// <returns></returns>
    public DecimalSize GetDecimalSize()
    {
        return Column.Table.Database.Server.Helper.GetQuerySyntaxHelper().TypeTranslater.GetDigitsBeforeAndAfterDecimalPointIfDecimal(SQLType);
    }

    /// <summary>
    /// Returns the System.Type that should be used to store values read out of columns of this data type (See <see cref="ITypeTranslater.GetCSharpTypeForSQLDBType"/>
    /// </summary>
    /// <returns></returns>
    public Type GetCSharpDataType()
    {
        return Column.Table.Database.Server.GetQuerySyntaxHelper().TypeTranslater.GetCSharpTypeForSQLDBType(SQLType);
    }

    /// <summary>
    /// Returns the <see cref="SQLType"/>
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return SQLType;
    }

    /// <summary>
    /// <para>Creates and runs an ALTER TABLE statement which will increase the size of a char column to support longer string values than it currently does.</para>
    /// 
    /// <para>Throws <see cref="InvalidResizeException"/> if the column is not a char type or the <paramref name="newSize"/> is smaller than the current column size</para>
    /// </summary>
    /// <param name="newSize"></param>
    /// <param name="managedTransaction"></param>
    /// <exception cref="InvalidResizeException"></exception>
    /// <exception cref="AlterFailedException"></exception>
    public void Resize(int newSize, IManagedTransaction managedTransaction = null)
    {
        var toReplace = GetLengthIfString();
            
        if(newSize == toReplace)
            return;

        if(newSize < toReplace)
            throw new InvalidResizeException(string.Format(FAnsiStrings.DiscoveredDataType_Resize_CannotResizeSmaller, SQLType, newSize));

        var newType = SQLType.Replace(toReplace.ToString(), newSize.ToString());

        AlterTypeTo(newType, managedTransaction);
    }

    /// <summary>
    /// <para>Creates and runs an ALTER TABLE statement which will increase the size of a decimal column to support larger Precision/Scale values than it currently does.
    /// If you want decimal(4,2) then pass <paramref name="numberOfDigitsBeforeDecimalPoint"/>=2 and <paramref name="numberOfDigitsAfterDecimalPoint"/>=2</para>
    /// 
    /// <para>Throws <see cref="InvalidResizeException"/> if the column is not a decimal type or the new size is smaller than the current column size</para>
    /// </summary>
    /// <param name="numberOfDigitsBeforeDecimalPoint">The number of decimal places before the . you want represented e.g. for decimal(5,3) specify 2</param>
    /// <param name="numberOfDigitsAfterDecimalPoint">The number of decimal places after the . you want represented e.g. for decimal(5,3,) specify 3</param>
    /// <param name="managedTransaction"></param>
    /// <exception cref="InvalidResizeException"></exception>
    /// <exception cref="AlterFailedException"></exception>
    public void Resize(int numberOfDigitsBeforeDecimalPoint, int numberOfDigitsAfterDecimalPoint, IManagedTransaction managedTransaction = null)
    {
        var toReplace = GetDecimalSize();

        if (toReplace == null || toReplace.IsEmpty)
            throw new InvalidResizeException(string.Format(FAnsiStrings.DiscoveredDataType_Resize_DataType_cannot_be_resized_to_decimal_because_it_is_of_data_type__0_, SQLType));

        if (toReplace.NumbersBeforeDecimalPlace > numberOfDigitsBeforeDecimalPoint)
            throw new InvalidResizeException(string.Format(FAnsiStrings.DiscoveredDataType_Resize_Cannot_shrink_column__number_of_digits_before_the_decimal_point_is_currently__0__and_you_asked_to_set_it_to__1___Current_SQLType_is__2__, toReplace.NumbersBeforeDecimalPlace, numberOfDigitsBeforeDecimalPoint, SQLType));

        if (toReplace.NumbersAfterDecimalPlace> numberOfDigitsAfterDecimalPoint)
            throw new InvalidResizeException(string.Format(FAnsiStrings.DiscoveredDataType_Resize_Cannot_shrink_column__number_of_digits_after_the_decimal_point_is_currently__0__and_you_asked_to_set_it_to__1___Current_SQLType_is__2__, toReplace.NumbersAfterDecimalPlace, numberOfDigitsAfterDecimalPoint, SQLType));
            
        var newDataType = Column.Table.GetQuerySyntaxHelper()
            .TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof (decimal), null,
                new DecimalSize(numberOfDigitsBeforeDecimalPoint, numberOfDigitsAfterDecimalPoint)));
            
        AlterTypeTo(newDataType, managedTransaction);
    }

    /// <summary>
    /// <para>Creates and runs an ALTER TABLE statement to change the data type to the <paramref name="newType"/></para>
    /// 
    /// <para>Consider using <see cref="Resize(int,FAnsi.Connections.IManagedTransaction)"/> instead</para>
    /// </summary>
    /// <param name="newType">The data type you want to change to e.g. "varchar(max)"</param>
    /// <param name="managedTransaction"></param>
    /// <param name="alterTimeoutInSeconds">The time to wait before giving up on the command (See <see cref="DbCommand.CommandTimeout"/></param>
    /// <exception cref="AlterFailedException"></exception>
    public void AlterTypeTo(string newType, IManagedTransaction managedTransaction = null,int alterTimeoutInSeconds = 500)
    {
        if(Column == null)
            throw new NotSupportedException(FAnsiStrings.DiscoveredDataType_AlterTypeTo_Cannot_resize_DataType_because_it_does_not_have_a_reference_to_a_Column_to_which_it_belongs);

        var server = Column.Table.Database.Server;
        using (var connection = server.GetManagedConnection(managedTransaction))
        {
            var sql = Column.Helper.GetAlterColumnToSql(Column, newType, Column.AllowNulls);
            try
            {
                using var cmd = server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
                cmd.CommandTimeout = alterTimeoutInSeconds;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw new AlterFailedException(string.Format(FAnsiStrings.DiscoveredDataType_AlterTypeTo_Failed_to_send_resize_SQL__0_, sql), e);
            }
        }

        SQLType = newType; 
    }

    /// <summary>
    /// Returns true if the <paramref name="other"/> describes the same <see cref="SQLType"/> as this
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    protected bool Equals(DiscoveredDataType other)
    {
        return string.Equals(SQLType, other.SQLType);
    }

    /// <summary>
    /// Equality based on <see cref="SQLType"/>
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode()
    {
        return SQLType != null ? SQLType.GetHashCode() : 0;
    }

    /// <summary>
    /// Equality based on <see cref="SQLType"/>
    /// </summary>
    /// <returns></returns>
    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((DiscoveredDataType)obj);
    }

}