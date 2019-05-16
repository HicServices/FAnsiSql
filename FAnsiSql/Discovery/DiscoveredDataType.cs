using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Connections;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Exceptions;

namespace FAnsi.Discovery
{
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
        public Dictionary<string, object> ProprietaryDatatype = new Dictionary<string, object>();

        /// <summary>
        /// API constructor, instead use <see cref="DiscoveredTable.DiscoverColumns"/> instead.
        /// </summary>
        /// <param name="r">All the values in r will be copied into the Dictionary property of this class called ProprietaryDatatype</param>
        /// <param name="sqlType">Your infered SQL data type for it e.g. varchar(50)</param>
        /// <param name="column">The column it belongs to, can be null e.g. if your datatype belongs to a DiscoveredParameter instead</param>
        public DiscoveredDataType(DbDataReader r, string sqlType, DiscoveredColumn column)
        {
            SQLType = sqlType;
            Column = column;

            for (int i = 0; i < r.FieldCount; i++)
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
            int toReplace = GetLengthIfString();
            
            if(newSize == toReplace)
                throw new InvalidResizeException("Why are you trying to resize a column that is already " + newSize + " long (" + SQLType + ")?");

            if(newSize < toReplace)
                throw new InvalidResizeException("You can only grow columns, you cannot shrink them with this method.  You asked to turn the current datatype from " + SQLType + " to reduced size " + newSize);

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
            DecimalSize toReplace = GetDecimalSize();

            if (toReplace == null || toReplace.IsEmpty)
                throw new InvalidResizeException("DataType cannot be resized to decimal because it is of data type " + SQLType);

            if (toReplace.NumbersBeforeDecimalPlace > numberOfDigitsBeforeDecimalPoint)
                throw new InvalidResizeException("Cannot shrink column, number of digits before the decimal point is currently " + toReplace.NumbersBeforeDecimalPlace + " and you asked to set it to " + numberOfDigitsBeforeDecimalPoint + " (Current SQLType is " + SQLType + ")");

            if (toReplace.NumbersAfterDecimalPlace> numberOfDigitsAfterDecimalPoint)
                throw new InvalidResizeException("Cannot shrink column, number of digits after the decimal point is currently " + toReplace.NumbersAfterDecimalPlace + " and you asked to set it to " + numberOfDigitsAfterDecimalPoint + " (Current SQLType is " + SQLType + ")");
            
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
        /// <param name="altertimeoutInSeconds">The time to wait before giving up on the command (See <see cref="DbCommand.CommandTimeout"/></param>
        /// <exception cref="AlterFailedException"></exception>
        public void AlterTypeTo(string newType, IManagedTransaction managedTransaction = null,int altertimeoutInSeconds = 500)
        {
            if(Column == null)
                throw new NotSupportedException("Cannot resize DataType because it does not have a reference to a Column to which it belongs (possibly you are trying to resize a data type associated with a TableValuedFunction Parameter?)");

            var server = Column.Table.Database.Server;
            using (var connection = server.GetManagedConnection(managedTransaction))
            {
                string sql = Column.Helper.GetAlterColumnToSql(Column, newType, Column.AllowNulls);
                try
                {
                    var cmd = server.Helper.GetCommand(sql, connection.Connection, connection.Transaction);
                    cmd.CommandTimeout = altertimeoutInSeconds;
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    throw new AlterFailedException("Failed to send resize SQL:" + sql, e);
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
            return (SQLType != null ? SQLType.GetHashCode() : 0);
        }

        /// <summary>
        /// Equality based on <see cref="SQLType"/>
        /// </summary>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((DiscoveredDataType)obj);
        }

    }
}
