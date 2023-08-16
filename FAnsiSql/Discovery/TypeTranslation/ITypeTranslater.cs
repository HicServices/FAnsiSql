using System;
using System.Data;
using TypeGuesser;

namespace FAnsi.Discovery.TypeTranslation;

/// <summary>
///  Cross database type functionality for translating between database proprietary datatypes e.g. varchar (varchar2 in Oracle) and the C# Type (and vice
///  versa).
/// 
/// <para>When translating into a database type from a C# Type you also need to know additonal information e.g. how long is the maximum length of a string, how much
/// scale/precision should a decimal have.  This is represented by the DatabaseTypeRequest class.</para>
/// 
/// </summary>
public interface ITypeTranslater
{
    /// <summary>
    ///  DatabaseTypeRequest is turned into the proprietary string e.g. A DatabaseTypeRequest with CSharpType = typeof(DateTime) is translated into
    /// 'datetime2' in Microsoft SQL Server but 'datetime' in MySql server.
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    string GetSQLDBTypeForCSharpType(DatabaseTypeRequest request);

    /// <summary>
    /// Returns the System.Data.DbType (e.g. DbType.String) for the specified proprietary database type (e.g. "varchar(max)")
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    /// <param name="sqlType"></param>
    /// <returns></returns>
    DbType GetDbTypeForSQLDBType(string sqlType);


    /// <summary>
    /// Translates a database proprietary type e.g. 'decimal(10,2)' into a C# type e.g. 'typeof(decimal)'
    /// </summary>
    /// <param name="sqlType"></param>
    /// <returns>The C# Type which can be used to store values of this database type</returns>
    Type GetCSharpTypeForSQLDBType(string sqlType);

    /// <summary>
    /// Translates a database proprietary type e.g. 'decimal(10,2)' into a C# type e.g. 'typeof(decimal)'
    /// 
    /// <para>Returns null if no the <paramref name="sqlType"/> is not understood</para>
    /// </summary>
    /// <param name="sqlType"></param>
    /// <returns>The C# Type which can be used to store values of this database type</returns>
    Type TryGetCSharpTypeForSQLDBType(string sqlType);

    /// <summary>
    /// Returns true if the <paramref name="sqlType"/> string could be reconciled into a known C# Type.  Do not use this
    /// for testing if a given random string is likely to be accepted by the DBMS.  You should only pass Types that actually
    /// resolve.
    /// 
    /// <para>See also <see cref="GetCSharpTypeForSQLDBType"/></para>
    /// </summary>
    /// <param name="sqlType">A DBMS type name which you want the API to guess a C# Type for.</param>
    /// <returns>True if FAnsi has a C# Type representation for the supplied <paramref name="sqlType"/></returns>
    bool IsSupportedSQLDBType(string sqlType);

    DatabaseTypeRequest GetDataTypeRequestForSQLDBType(string sqlType);

    Guesser GetGuesserFor(DiscoveredColumn discoveredColumn);

    int GetLengthIfString(string sqlType);
    DecimalSize GetDigitsBeforeAndAfterDecimalPointIfDecimal(string sqlType);

    /// <summary>
    /// Translates the given sqlType which must be an SQL string compatible with this TypeTranslater e.g. varchar(10) into the destination ITypeTranslater
    /// e.g. Varchar2(10) if destinationTypeTranslater was Oracle.  Even if both this and the destination are the same you might find a different datatype
    /// due to translation preference and Type merging e.g. text might change to varchar(max)
    /// </summary>
    /// <param name="sqlType"></param>
    /// <param name="destinationTypeTranslater"></param>
    /// <returns></returns>
    string TranslateSQLDBType(string sqlType, ITypeTranslater destinationTypeTranslater);
}