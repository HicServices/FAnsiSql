using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using FAnsi.Discovery.QuerySyntax.Update;
using FAnsi.Discovery.TypeTranslation;
using TypeGuesser;

namespace FAnsi.Discovery
{
    /// <inheritdoc/>
    public abstract class QuerySyntaxHelper : IQuerySyntaxHelper
    {
        public virtual string DatabaseTableSeparator => ".";
        
        public abstract int MaximumDatabaseLength { get; }
        public abstract int MaximumTableLength { get; }
        public abstract int MaximumColumnLength { get; }

        public virtual char[] IllegalNameChars { get; } = new []{'.','(',')'};

        /// <summary>
        /// Regex for identifying parameters in blocks of SQL (starts with @ or : (Oracle)
        /// </summary>
        /// <returns></returns>
        protected static Regex ParameterNamesRegex = new Regex("([@:][A-Za-z0-9_]*)\\s?", RegexOptions.IgnoreCase);

        /// <summary>
        /// Symbols (for all database types) which denote wrapped entity names e.g. [dbo].[mytable] contains qualifiers '[' and ']'
        /// </summary>
        public static char[] TableNameQualifiers = { '[', ']', '`' ,'"'};

        public ITypeTranslater TypeTranslater { get; private set; }
        
        private readonly Dictionary<CultureInfo,TypeDeciderFactory> factories = new Dictionary<CultureInfo, TypeDeciderFactory>();

        public IAggregateHelper AggregateHelper { get; private set; }
        public IUpdateHelper UpdateHelper { get; set; }
        public DatabaseType DatabaseType { get; private set; }

        public virtual char ParameterSymbol { get { return '@'; } }

        
        /// <summary>
        /// Returns a regex that picks up alias specifications in SELECT sql (e.g. "mytbl.mycol as fish").  This only has to match when the
        /// " AS " qualifier is used explicitly.  The capture groups of this Regex must match <see cref="SplitLineIntoSelectSQLAndAlias"/>
        /// </summary>
        /// <returns></returns>
        protected virtual Regex GetAliasRegex()
        {
            //whitespace followed by as and more whitespace
            //Then any word (optionally bounded by a table name qualifier)

            //alias is a word
            //(w+)
            
            //alias is a wrapped word e.g. [hey hey].  In this case we must allow anything between the brackets that is not closing bracket
            //[[`""]([^[`""]+)[]`""]

            return new Regex(@"\s+as\s+((\w+)|([[`""]([^[`""]+)[]`""]))$", RegexOptions.IgnoreCase);
        }

        protected virtual string GetAliasConst()
        {
            return " AS ";
        }
        
        public string AliasPrefix
        {
            get
            {
                return GetAliasConst();
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

            //if it is an aliased entity e.g. AS fish then we should return fish (this is the case for table valued functions and not much else)
            if (SplitLineIntoSelectSQLAndAlias(s.Trim(), out _, out string alias))
                return alias;

            //it doesn't have an alias, e.g. it's `MyDatabase`.`mytable` or something

            //if it's "count(1)" or something then that's a problem!
            if (s.IndexOfAny(new char[]{'(',')' }) != -1)
                throw new RuntimeNameException("Could not determine runtime name for Sql:'" + s + "'.  It had brackets and no alias.  Try adding ' as mycol' to the end.");

            return s.Substring(s.LastIndexOf(".") + 1).Trim('[', ']', '`','"');
        }
        
        public virtual bool TryGetRuntimeName(string s,out string name)
        {
            try
            {
                name = GetRuntimeName(s);
                return true;
            }
            catch (RuntimeNameException)
            {
                name = null;
                return false;
            }
        }

        public string EnsureWrapped(string databaseOrTableName)
        {
            if (databaseOrTableName.Contains(DatabaseTableSeparator))
                throw new Exception(string.Format(FAnsiStrings.QuerySyntaxHelper_EnsureWrapped_String_passed_to_EnsureWrapped___0___contained_separators__not_allowed____Prohibited_Separator_is___1__,databaseOrTableName, DatabaseTableSeparator));

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

        public virtual HashSet<string> GetReservedWords()
        {
            return new HashSet<string>(StringComparer.CurrentCultureIgnoreCase);
        }

        public abstract string GetParameterDeclaration(string proposedNewParameterName, string sqlType);

        /// <summary>
        /// Splits the given <paramref name="lineToSplit"/> into 
        /// </summary>
        /// <param name="lineToSplit"></param>
        /// <param name="selectSQL"></param>
        /// <param name="alias"></param>
        /// <returns></returns>
        public virtual bool SplitLineIntoSelectSQLAndAlias(string lineToSplit, out string selectSQL, out string alias)
        {
            //Ths line is expected to be some SELECT sql so remove trailing whitespace and commas etc
            lineToSplit = lineToSplit.TrimEnd(',', ' ', '\n', '\r');

            var matches = GetAliasRegex().Matches(lineToSplit);

            if (matches.Count >1)
                throw new SyntaxErrorException(string.Format(FAnsiStrings.QuerySyntaxHelper_SplitLineIntoSelectSQLAndAlias_,matches.Count,lineToSplit));

            if (matches.Count == 0)
            {
                selectSQL = lineToSplit;
                alias = null;
                return false;
            }

            //match is an unwrapped alias
            string unqualifiedAlias = matches[0].Groups[2].Value;
            string qualifiedAlias = matches[0].Groups[4].Value;

            alias = string.IsNullOrWhiteSpace(unqualifiedAlias) ? qualifiedAlias : unqualifiedAlias;
            alias = alias.Trim();
            selectSQL = lineToSplit.Substring(0, matches[0].Index).Trim();
            return true;
        }

        public abstract string GetScalarFunctionSql(MandatoryScalarFunctions function);

        /// <inheritdoc/>
        public void SplitLineIntoOuterMostMethodAndContents(string lineToSplit, out string method, out string contents)
        {
            if (string.IsNullOrWhiteSpace(lineToSplit))
                throw new ArgumentException(
                    FAnsiStrings.QuerySyntaxHelper_SplitLineIntoOuterMostMethodAndContents_Line_must_not_be_blank,
                    nameof(lineToSplit));

            if (lineToSplit.Count(c => c.Equals('(')) != lineToSplit.Count(c => c.Equals(')')))
                throw new ArgumentException(
                    FAnsiStrings
                        .QuerySyntaxHelper_SplitLineIntoOuterMostMethodAndContents_The_number_of_opening_and_closing_parentheses_must_match,
                    nameof(lineToSplit));

            int firstBracket = lineToSplit.IndexOf('(');

            if (firstBracket == -1)
                throw new ArgumentException(
                    FAnsiStrings
                        .QuerySyntaxHelper_SplitLineIntoOuterMostMethodAndContents_Line_must_contain_at_least_one_pair_of_parentheses,
                    nameof(lineToSplit));

            method = lineToSplit.Substring(0, firstBracket).Trim();

            int lastBracket = lineToSplit.LastIndexOf(')');

            int length = lastBracket - (firstBracket + 1);

            if (length == 0)
                contents = ""; //it's something like count()
            else
                contents = lineToSplit.Substring(firstBracket + 1, length).Trim();
        }

        public static string MakeHeaderNameSensible(string header)
        {
            if (string.IsNullOrWhiteSpace(header))
                return header;

            //replace anything that isn't a digit, letter or underscore with emptiness (except spaces - these will go but first...)
            //also accept anything above ASCII 256
            Regex r = new Regex("[^A-Za-z0-9_ \u0101-\uFFFF]");

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

        public string GetSensibleEntityNameFromString(string potentiallyDodgyName)
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

            /*
            //todo doesn't work with .net standard 
            var oleE = exception as OleDbException;

            if (oleE != null && oleE.ErrorCode == -2147217871)
                return true;*/

            return exception.Message.ToLower().Contains("timeout");
        }

        public abstract string HowDoWeAchieveMd5(string selectSql);
        
        

        public DbParameter GetParameter(DbParameter p, DiscoveredColumn discoveredColumn, object value,CultureInfo culture)
        {
            try
            {
                if(culture == null)
                    culture = CultureInfo.CurrentCulture;
                
                if(!factories.ContainsKey(culture))
                    factories.Add(culture,new TypeDeciderFactory(culture));

                var tt = TypeTranslater;
                p.DbType = tt.GetDbTypeForSQLDBType(discoveredColumn.DataType.SQLType);
                var cSharpType = tt.GetCSharpTypeForSQLDBType(discoveredColumn.DataType.SQLType);

                if (IsBasicallyNull(value))
                    p.Value = DBNull.Value;
                else
                    if (value is string strVal && factories[culture].IsSupported(cSharpType)) //if the input is a string and it's for a hard type e.g. TimeSpan 
                    {
                        var decider = factories[culture].Create(cSharpType);
                        var o = decider.Parse(strVal);

                        //Not all DBMS support DBParameter.Value = new TimeSpan(...);
                        if (o is TimeSpan)
                            o = FormatTimespanForDbParameter((TimeSpan) o);

                        p.Value = o;

                    }
                    else
                        p.Value = value;
            }
            catch(Exception ex)
            {
                throw new Exception(string.Format(FAnsiStrings.QuerySyntaxHelper_GetParameter_Could_not_GetParameter_for_column___0__, discoveredColumn.GetFullyQualifiedName()),ex);
            }            

            return p;
        }

        public void ValidateDatabaseName(string databaseName)
        {
            if(!IsValidDatabaseName(databaseName,out string reason))
                throw new RuntimeNameException(reason);
        }
        public void ValidateTableName(string tableName)
        {
            if(!IsValidTableName(tableName,out string reason))
                throw new RuntimeNameException(reason);
        }
        public void ValidateColumnName(string columnName)
        {
            if(!IsValidColumnName(columnName,out string reason))
                throw new RuntimeNameException(reason);
        }

        public bool IsValidDatabaseName(string databaseName,out string reason)
        {
            reason = ValidateName(databaseName, "Database", MaximumDatabaseLength);
            return string.IsNullOrWhiteSpace(reason);
        }
        
        public bool IsValidTableName(string tableName,out string reason)
        {
            reason = ValidateName(tableName, "Table", MaximumTableLength);
            return string.IsNullOrWhiteSpace(reason);
        }

        public bool IsValidColumnName(string columnName,out string reason)
        {
            reason = ValidateName(columnName, "Column", MaximumColumnLength);
            return string.IsNullOrWhiteSpace(reason);
        }

        public virtual string GetDefaultSchemaIfAny()
        {
            return null;
        }

        /// <summary>
        /// returns null if the name is valid.  Otherwise a string describing why it is invalid.
        /// </summary>
        /// <param name="candidate"></param>
        /// <param name="objectType">Type of object being validated e.g. "Database", "Table" etc</param>
        /// <param name="maximumLengthAllowed"></param>
        /// <returns></returns>
        private string ValidateName(string candidate, string objectType, int maximumLengthAllowed)
        {
            if(string.IsNullOrWhiteSpace(candidate))
                return string.Format(FAnsiStrings.QuerySyntaxHelper_ValidateName__0__name_cannot_be_blank, objectType);

            if(candidate.Length > maximumLengthAllowed)
                return string.Format(FAnsiStrings.QuerySyntaxHelper_ValidateName__0__name___1___is_too_long_for_the_DBMS___2__supports_maximum_length_of__3__,
                    objectType, candidate.Substring(0, maximumLengthAllowed), DatabaseType, maximumLengthAllowed);

            if(candidate.IndexOfAny(IllegalNameChars) != -1)
                return string.Format(
                    FAnsiStrings.QuerySyntaxHelper_ValidateName__0__name___1___contained_unsupported__by_FAnsi__characters___Unsupported_characters_are__2_,
                    objectType, candidate, new string(IllegalNameChars));

            return null;
        }

        

        public DbParameter GetParameter(DbParameter p, DiscoveredColumn discoveredColumn, object value)
        {
            return GetParameter(p,discoveredColumn,value,null);
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
            if (obj is null) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((QuerySyntaxHelper)obj);
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode();
        }
        #endregion

        public Dictionary<T, string> GetParameterNamesFor<T>(T[] columns, Func<T,string> toStringFunc)
        {
            var toReturn = new Dictionary<T, string>();
            
            var reservedKeywords = GetReservedWords();
                       

            //sensible parameter names have no spaces or symbols!
            Regex sensibleParameterNamesInclude = new Regex(@"^\w*$");

            for (int i = 0; i < columns.Length; i++)
            {
                T c = columns[i];
                var columnName = toStringFunc(c);
                
                if(!sensibleParameterNamesInclude.IsMatch(columnName)) //if column name is "_:_" or something
                    toReturn.Add(c,ParameterSymbol + "p"+i);
                else
                    toReturn.Add(c,ParameterSymbol + (reservedKeywords.Contains(columnName)?columnName +"1":columnName)); //if column is reserved keyword or normal name
            }

            return toReturn;
        }
    }
}
