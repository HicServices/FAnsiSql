using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using FAnsi.Discovery.QuerySyntax.Update;
using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Discovery.QuerySyntax
{
    /// <summary>
    /// Cross database type functionality for helping build SQL commands that will work regardless of DatabaseType (Microsoft Sql Server / MySql etc).  Describes
    /// how to translate broad requirements like 'database type capable of storing strings up to 10 characters long' into a specific implementation e.g. 
    /// 'varchar(10)' in Microsoft SQL Server and 'varchar2(10)' in Oracle (See ITypeTranslater).
    /// 
    /// <para>Also includes features such as qualifying database entities [MyDatabase]..[MyTable].[MyColumn] in Sql Server vs `MyDatabase`.`MyTable`.`MyColumn` in 
    /// MySql.</para>
    /// 
    /// <para>Also includes methods for dealing with no n Ansi standard functionality e.g. TOP X , MD5 etc</para>
    /// 
    /// </summary>
    public interface IQuerySyntaxHelper
    {
        ITypeTranslater TypeTranslater { get; }

        /// <summary>
        /// Creates parameters names from the column names in the collection.  Use this for INSERT etc commands to avoid SQL injection and handle creepy column
        /// names with spaces or reserved keywords
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="toStringFunc">Function to convert the <typeparamref name="T"/> to a string e.g. c.ColumnName if DataColumn</param>
        /// <returns></returns>
        Dictionary<T, string> GetParameterNamesFor<T>(T[] columns, Func<T,string> toStringFunc);

        IAggregateHelper AggregateHelper { get; }
        IUpdateHelper UpdateHelper { get; set; }

        char ParameterSymbol { get; }

        string GetRuntimeName(string s);

        bool TryGetRuntimeName(string s, out string name);

        DatabaseType DatabaseType {get;}
        
        /// <summary>
        /// Ensures that the supplied single entity object e.g. "mytable" , "mydatabase, "[mydatabase]", "`mydatabase` etc is returned wrapped in appropriate qualifiers for
        /// the database we are providing syntax for
        /// </summary>
        /// <param name="databaseOrTableName"></param>
        /// <returns></returns>
        string EnsureWrapped(string databaseOrTableName);

        string EnsureFullyQualified(string databaseName,string schemaName, string tableName);
        string EnsureFullyQualified(string databaseName, string schemaName,string tableName, string columnName, bool isTableValuedFunction = false);
        string Escape(string sql);

        TopXResponse HowDoWeAchieveTopX(int x);
        string GetParameterDeclaration(string proposedNewParameterName, DatabaseTypeRequest request);
        string GetParameterDeclaration(string proposedNewParameterName, string sqlType);
        
        bool IsValidParameterName(string parameterSQL);

        string AliasPrefix { get; }
        
        /// <summary>
        /// The maximum number of characters allowed in database names according to the DBMS
        /// </summary>
        int MaximumDatabaseLength { get; }

        /// <summary>
        /// The maximum number of characters allowed in table names according to the DBMS
        /// </summary>
        int MaximumTableLength { get; }

        /// <summary>
        /// The maximum number of characters allowed in column names according to the DBMS
        /// </summary>
        int MaximumColumnLength { get; }


        bool SplitLineIntoSelectSQLAndAlias(string lineToSplit, out string selectSQL, out string alias);

        string GetScalarFunctionSql(MandatoryScalarFunctions function);
        string GetSensibleTableNameFromString(string potentiallyDodgyName);
        
        /// <summary>
        /// The SQL that would be valid for a CREATE TABLE statement that would result in a given column becoming auto increment e.g. "IDENTITY(1,1)"
        /// </summary>
        /// <returns></returns>
        string GetAutoIncrementKeywordIfAny();

        /// <summary>
        /// Get a list of functions to SQL code (including parameter names).  This is used primarily in autocomplete situations where the user wants to
        /// know the available functions within the targeted dbms.
        /// </summary>
        /// <returns></returns>
        Dictionary<string, string> GetSQLFunctionsDictionary();

        bool IsBasicallyNull(object value);
        bool IsTimeout(Exception exception);
        
        HashSet<string> GetReservedWords();

        /// <summary>
        /// Returns SQL that will wrap a single line of SQL with the SQL to calculate MD5 hash e.g. change `MyTable`.`MyColumn` to md5(`MyTable`.`MyColumn`)
        /// <para>The SQL might include transform functions e.g. UPPER etc</para>
        /// </summary>
        /// <param name="selectSql"></param>
        /// <returns></returns>
        string HowDoWeAchieveMd5(string selectSql);

        /// <summary>
        /// Gets a DbParameter hard typed with the correct DbType for the discoveredColumn and the Value set to the correct Value representation (e.g. DBNull for nulls or whitespace).
        /// <para>Also handles converting DateTime representations since many DBMS are a bit rubbish at that</para> 
        /// </summary>
        /// <param name="p"></param>
        /// <param name="discoveredColumn">The column the parameter is for loading - this is used to determine the DbType for the paramter</param>
        /// <param name="value">The value to populate into the command, this will be converted to DBNull.Value if the value is nullish</param>
        /// <returns></returns>
        DbParameter GetParameter(DbParameter p, DiscoveredColumn discoveredColumn,object value);

        /// <summary>
        /// Gets a DbParameter hard typed with the correct DbType for the discoveredColumn and the Value set to the correct Value representation (e.g. DBNull for nulls or whitespace).
        /// <para>Also handles converting DateTime representations since many DBMS are a bit rubbish at that</para> 
        /// </summary>
        /// <param name="p"></param>
        /// <param name="discoveredColumn">The column the parameter is for loading - this is used to determine the DbType for the paramter</param>
        /// <param name="value">The value to populate into the command, this will be converted to DBNull.Value if the value is nullish</param>
        /// <param name="culture"></param>
        /// <returns></returns>
        DbParameter GetParameter(DbParameter p, DiscoveredColumn discoveredColumn,object value,CultureInfo culture);
    }

    public enum MandatoryScalarFunctions
    {
        None = 0,

        /// <summary>
        /// A scalar function which must return todays datetime.  Must be valid as a column default too
        /// </summary>
        GetTodaysDate,
        
        /// <summary>
        /// A scalar function which must return a new random GUID.
        /// </summary>
        GetGuid
    }
}
