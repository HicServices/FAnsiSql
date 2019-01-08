﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using FAnsi.Discovery.QuerySyntax.Update;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Discovery.TypeTranslation.TypeDeciders;

namespace FAnsi.Discovery
{
    /// <inheritdoc/>
    public abstract class QuerySyntaxHelper : IQuerySyntaxHelper
    {
        public abstract string DatabaseTableSeparator { get; }
        
        /// <summary>
        /// Regex for identifying parameters in blocks of SQL (starts with @ or : (Oracle)
        /// </summary>
        /// <returns></returns>
        protected static Regex ParameterNamesRegex = new Regex("([@:][A-Za-z0-9_]*)\\s?", RegexOptions.IgnoreCase);

        /// <summary>
        /// Symbols (for all database types) which denote wrapped entity names e.g. [dbo].[mytable] contains qualifiers '[' and ']'
        /// </summary>
        public static char[] TableNameQualifiers = { '[', ']', '`' };

        public ITypeTranslater TypeTranslater { get; private set; }
        readonly TypeDeciderFactory typeDeciderFactory = new TypeDeciderFactory();

        public IAggregateHelper AggregateHelper { get; private set; }
        public IUpdateHelper UpdateHelper { get; set; }
        public DatabaseType DatabaseType { get; private set; }

        public virtual char ParameterSymbol { get { return '@'; } }

        
        protected virtual Regex GetAliasRegex()
        {
            return new Regex(@"\s+as\s+(\w+)$", RegexOptions.IgnoreCase);
        }

        protected virtual string GetAliasConst()
        {
            return " AS ";
        }
        
        public string AliasPrefix
        {
            get
            {
                return ValidateAlias(GetAliasConst());
            }
        }

        /// <summary>
        /// Lists the names of all parameters required by the supplied whereSql e.g. @bob = 'bob' would return "@bob" 
        /// </summary>
        /// <param name="query">the SQL you want to determine the parameter names in</param>
        /// <returns>parameter names that are required by the SQL</returns>
        public static HashSet<string> GetAllParameterNamesFromQuery(string query)
        {
            //Only look at the start of the string or following an equals or whitespace and stop at word boundaries
            var regex = new Regex(@"(?:^|[\s+\-*/\\=(])+" + ParameterNamesRegex + @"\b");

            var toReturn = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

            if (string.IsNullOrWhiteSpace(query))
                return toReturn;

            foreach (Match match in regex.Matches(query))
                if (!toReturn.Contains(match.Value.Trim())) //dont add duplicates
                    toReturn.Add(match.Groups[1].Value.Trim());

            return toReturn;
        }

        /// <inheritdoc />
        public static string GetParameterNameFromDeclarationSQL(string parameterSQL)
        {
            if (!ParameterNamesRegex.IsMatch(parameterSQL))
                throw new Exception("ParameterSQL does not match regex pattern:" + ParameterNamesRegex);

            return ParameterNamesRegex.Match(parameterSQL).Value.Trim();
        }

        public bool IsValidParameterName(string parameterSQL)
        {
            return ParameterNamesRegex.IsMatch(parameterSQL);
        }

        protected QuerySyntaxHelper(ITypeTranslater translater, IAggregateHelper aggregateHelper, IUpdateHelper updateHelper, DatabaseType databaseType)
        {
            TypeTranslater = translater;
            AggregateHelper = aggregateHelper;
            UpdateHelper = updateHelper;
            DatabaseType = databaseType;
        }

        public virtual string GetRuntimeName(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return s;

            var match = GetAliasRegex().Match(s.Trim());//if it is an aliased entity e.g. AS fish then we should return fish (this is the case for table valued functions and not much else)
            if (match.Success)
                return match.Groups[1].Value;

            return s.Substring(s.LastIndexOf(".") + 1).Trim('[', ']', '`');
        }


        public string EnsureWrapped(string databaseOrTableName)
        {
            if (databaseOrTableName.Contains(DatabaseTableSeparator))
                throw new Exception("Strings passed to EnsureWrapped cannot contain separators i.e. '" + DatabaseTableSeparator + "'");

            return EnsureWrappedImpl(databaseOrTableName);
        }

        public abstract string EnsureWrappedImpl(string databaseOrTableName);

        public virtual string EnsureFullyQualified(string databaseName, string schema, string tableName)
        {

            string toReturn = GetRuntimeName(databaseName);

            if (!string.IsNullOrWhiteSpace(schema))
                toReturn += "." + schema;

            toReturn += "." + GetRuntimeName(tableName);

            return toReturn;
        }

        public virtual string EnsureFullyQualified(string databaseName, string schema, string tableName, string columnName, bool isTableValuedFunction = false)
        {
            if (isTableValuedFunction)
                return GetRuntimeName(tableName) + "." + GetRuntimeName(columnName);//table valued functions do not support database name being in the column level selection list area of sql queries

            return EnsureFullyQualified(databaseName, schema, tableName) + "." + GetRuntimeName(columnName);
        }

        public virtual string Escape(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return sql;

            return sql.Replace("'", "''");
        }
        public abstract TopXResponse HowDoWeAchieveTopX(int x);

        public virtual string GetParameterDeclaration(string proposedNewParameterName, DatabaseTypeRequest request)
        {
            return GetParameterDeclaration(proposedNewParameterName, TypeTranslater.GetSQLDBTypeForCSharpType(request));
        }

        public abstract string GetParameterDeclaration(string proposedNewParameterName, string sqlType);

        private string ValidateAlias(string getAlias)
        {
            if (!(getAlias.StartsWith(" ") && getAlias.EndsWith(" ")))
                throw new NotSupportedException("GetAliasConst method on Type " + this.GetType().Name + " returned a value that was not bounded by whitespace ' '.  GetAliasConst must start and end with a space e.g. ' AS '");

            var testString = "col " + getAlias + " bob";
            var match = GetAliasRegex().Match(testString);
            if (!match.Success)
                throw new NotSupportedException("GetAliasConst method on Type " + this.GetType().Name + " returned a value that was not matched by  GetAliasRegex()");

            if (match.Groups.Count < 2 || !match.Groups[1].Value.Equals("bob"))
                throw new NotSupportedException("GetAliasRegex method on Type " + this.GetType().Name + @" must return a regex with a capture group that matches the runtime name of the line e.g. \s+AS\s+(\w+)$");


            return getAlias;
        }

        public virtual bool SplitLineIntoSelectSQLAndAlias(string lineToSplit, out string selectSQL, out string alias)
        {
            StringComparison comparisonType = StringComparison.InvariantCultureIgnoreCase;

            if (lineToSplit.IndexOf(AliasPrefix, comparisonType) == -1)
            {
                //doesn't have the alias prefix
                selectSQL = lineToSplit.TrimEnd(',', ' ', '\n', '\r');
                alias = null;
                return false;
            }

            if (lineToSplit.IndexOf(AliasPrefix, comparisonType) != lineToSplit.LastIndexOf(AliasPrefix, comparisonType))
                throw new SyntaxErrorException("Found two instances of the alias prefix:\"" + AliasPrefix + "\"");

            int splitPoint = lineToSplit.IndexOf(AliasPrefix, comparisonType);

            selectSQL = lineToSplit.Substring(0, splitPoint);

            //could end with the alias and then be blank if the user is busy typing it all in a oner
            if (splitPoint + AliasPrefix.Length < lineToSplit.Length)
            {
                alias = lineToSplit.Substring(splitPoint + AliasPrefix.Length).TrimEnd(',', ' ', '\n', '\r');

                return true;
            }

            alias = null;
            return false;
        }

        public abstract string GetScalarFunctionSql(MandatoryScalarFunctions function);

        /// <summary>
        /// Takes a line line " count(*) " and returns "count" and "*"
        /// Also handles LTRIM(RTRIM(FishFishFish)) by returning "LTRIM" and  "RTRIM(FishFishFish)"
        /// </summary>
        /// <param name="lineToSplit"></param>
        /// <param name="method"></param>
        /// <param name="contents"></param>
        public void SplitLineIntoOuterMostMethodAndContents(string lineToSplit, out string method, out string contents)
        {
            if (string.IsNullOrWhiteSpace(lineToSplit))
                throw new ArgumentException("line must not be blank", lineToSplit);

            if (lineToSplit.Count(c => c.Equals('(')) != lineToSplit.Count(c => c.Equals(')')))
                throw new ArgumentException("The number of opening parentheses must match the number of closing parentheses", "lineToSplit");

            int firstBracket = lineToSplit.IndexOf('(');

            if (firstBracket == -1)
                throw new ArgumentException("Line must contain at least one pair of parentheses", "lineToSplit");

            method = lineToSplit.Substring(0, firstBracket).Trim();

            int lastBracket = lineToSplit.LastIndexOf(')');

            int length = lastBracket - (firstBracket + 1);

            if (length == 0)
                contents = ""; //it's something like count()
            else
                contents = lineToSplit.Substring(firstBracket + 1, length).Trim();
        }

        public static string MakeHeaderNameSane(string header)
        {
            if (string.IsNullOrWhiteSpace(header))
                return header;

            //replace anything that isn't a digit, letter or underscore with emptiness (except spaces - these will go but first...)
            Regex r = new Regex("[^A-Za-z0-9_ ]");

            string adjustedHeader = r.Replace(header, "");

            StringBuilder sb = new StringBuilder(adjustedHeader);

            //Camel case after spaces
            for (int i = 0; i < sb.Length; i++)
            {
                //if we are looking at a space
                if (sb[i] == ' ')
                    if (i + 1 < sb.Length) //and there is another character 
                        if (sb[i + 1] >= 'a' && sb[i + 1] <= 'z') //and that character is a lower case letter
                            sb[i + 1] = char.ToUpper(sb[i + 1]);
            }

            adjustedHeader = sb.ToString().Replace(" ", "");

            //if it starts with a digit (illegal) put an underscore before it
            if (Regex.IsMatch(adjustedHeader, "^[0-9]"))
                adjustedHeader = "_" + adjustedHeader;

            return adjustedHeader;
        }

        public string GetSensibleTableNameFromString(string potentiallyDodgyName)
        {
            potentiallyDodgyName = GetRuntimeName(potentiallyDodgyName);

            //replace anything that isn't a digit, letter or underscore with underscores
            Regex r = new Regex("[^A-Za-z0-9_]");
            string adjustedHeader = r.Replace(potentiallyDodgyName, "_");

            //if it starts with a digit (illegal) put an underscore before it
            if (Regex.IsMatch(adjustedHeader, "^[0-9]"))
                adjustedHeader = "_" + adjustedHeader;

            return adjustedHeader;
        }

        public abstract string GetAutoIncrementKeywordIfAny();
        public abstract Dictionary<string, string> GetSQLFunctionsDictionary();

        public bool IsBasicallyNull(object value)
        {
            if (value is string)
                return string.IsNullOrWhiteSpace((string)value);

            return value == null || value == DBNull.Value;
        }

        public virtual bool IsTimeout(Exception exception)
        {

            var oleE = exception as OleDbException;

            if (oleE != null && oleE.ErrorCode == -2147217871)
                return true;

            return exception.Message.ToLower().Contains("timeout");
        }

        public abstract string HowDoWeAchieveMd5(string selectSql);


        public DbParameter GetParameter(DbParameter p, DiscoveredColumn discoveredColumn, object value)
        {
            var tt = TypeTranslater;
            p.DbType = tt.GetDbTypeForSQLDBType(discoveredColumn.DataType.SQLType);
            var cSharpType = tt.GetCSharpTypeForSQLDBType(discoveredColumn.DataType.SQLType);

            if (IsBasicallyNull(value))
                p.Value = DBNull.Value;
            else
                if (value is string && typeDeciderFactory.IsSupported(cSharpType)) //if the input is a string and it's for a hard type e.g. TimeSpan 
                {
                    var o = typeDeciderFactory.Create(cSharpType).Parse((string)value);

                    //Not all DBMS support DBParameter.Value = new TimeSpan(...);
                    if (o is TimeSpan)
                        o = FormatTimespanForDbParameter((TimeSpan) o);

                    p.Value = o;

                }
                else
                    p.Value = value;

            return p;
        }

        /// <summary>
        /// Return the appropriate value such that it can be put into a DbParameter.Value field and be succesfully inserted into a
        /// column in the database designed to represent time fields (without date).
        /// </summary>
        /// <param name="timeSpan"></param>
        /// <returns></returns>
        protected virtual object FormatTimespanForDbParameter(TimeSpan timeSpan)
        {
            return timeSpan;
        }

        #region Equality Members
        protected bool Equals(QuerySyntaxHelper other)
        {
            if (other == null)
                return false;

            return GetType() == other.GetType();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((QuerySyntaxHelper)obj);
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode();
        }
        #endregion
    }
}
