using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using FAnsi.Discovery.QuerySyntax.Update;
using FAnsi.Discovery.TypeTranslation;
using TypeGuesser;

namespace FAnsi.Discovery;

/// <inheritdoc/>
public abstract partial class QuerySyntaxHelper(
    ITypeTranslater translater,
    IAggregateHelper aggregateHelper,
    IUpdateHelper updateHelper,
    DatabaseType databaseType)
    : IQuerySyntaxHelper
{
    private static readonly System.Buffers.SearchValues<char> BracketSearcher = System.Buffers.SearchValues.Create("()");

    public virtual string DatabaseTableSeparator => ".";

    /// <inheritdoc/>
    public abstract int MaximumDatabaseLength { get; }

    /// <inheritdoc/>
    public abstract int MaximumTableLength { get; }

    /// <inheritdoc/>
    public abstract int MaximumColumnLength { get; }

    /// <inheritdoc/>
    public virtual char[] IllegalNameChars { get; } = ['.', '(', ')'];

    public abstract string False { get; }
    public abstract string True { get; }

    /// <summary>
    /// Regex for identifying parameters in blocks of SQL (starts with @ or : (Oracle)
    /// </summary>
    /// <returns></returns>
    private static readonly Regex ParameterNamesRegex = ParameterNamesRe();

    /// <summary>
    /// Symbols (for all database types) which denote wrapped entity names e.g. [dbo].[mytable] contains qualifiers '[' and ']'
    /// </summary>
    public static readonly char[] TableNameQualifiers = ['[', ']', '`', '"'];

    /// <inheritdoc/>
    public abstract string OpenQualifier { get; }

    /// <inheritdoc/>
    public abstract string CloseQualifier { get; }

    public ITypeTranslater TypeTranslater { get; private set; } = translater;

    private readonly Dictionary<CultureInfo, TypeDeciderFactory> factories = [];

    public IAggregateHelper AggregateHelper { get; private set; } = aggregateHelper;
    public IUpdateHelper UpdateHelper { get; set; } = updateHelper;
    public DatabaseType DatabaseType { get; private set; } = databaseType;

    public virtual char ParameterSymbol => '@';

    private static string GetAliasConst() => " AS ";

    public string AliasPrefix => GetAliasConst();

    //Only look at the start of the string or following an equals or white space and stop at word boundaries
    private static readonly Regex ParameterNameRegex = new($@"(?:^|[\s+\-*/\\=(,])+{ParameterNamesRegex}\b");

    /// <summary>
    /// Lists the names of all parameters required by the supplied whereSql e.g. @bob = 'bob' would return "@bob"
    /// </summary>
    /// <param name="query">the SQL you want to determine the parameter names in</param>
    /// <returns>parameter names that are required by the SQL</returns>
    public static HashSet<string> GetAllParameterNamesFromQuery(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return [];

        var toReturn = new HashSet<string>(ParameterNameRegex.Matches(query).Cast<Match>().Select(static match => match.Groups[1].Value.Trim()), StringComparer.InvariantCultureIgnoreCase);
        return toReturn;
    }

    public static string GetParameterNameFromDeclarationSQL(string parameterSQL)
    {
        if (!ParameterNamesRegex.IsMatch(parameterSQL))
            throw new Exception($"ParameterSQL does not match regex pattern:{ParameterNamesRegex}");

        return ParameterNamesRegex.Match(parameterSQL).Value.Trim();
    }

    public bool IsValidParameterName(string parameterSQL) => ParameterNamesRegex.IsMatch(parameterSQL);

    [return: NotNullIfNotNull(nameof(s))]
    public virtual string? GetRuntimeName(string? s)
    {
        if (string.IsNullOrWhiteSpace(s))
            return s;

        //if it is an aliased entity e.g. AS fish then we should return fish (this is the case for table valued functions and not much else)
        if (SplitLineIntoSelectSQLAndAlias(s.Trim(), out _, out var alias))
            return alias;

        //it doesn't have an alias, e.g. it's `MyDatabase`.`mytable` or something

        //if it's "count(1)" or something then that's a problem!
        if (s.AsSpan().IndexOfAny(BracketSearcher) != -1)
            throw new RuntimeNameException(
                $"Could not determine runtime name for Sql:'{s}'.  It had brackets and no alias.  Try adding ' as mycol' to the end.");

        //Last symbol with no whitespace
        var lastWord = s[(s.LastIndexOf('.') + 1)..].Trim();

        if (string.IsNullOrWhiteSpace(lastWord) || lastWord.Length < 2)
            return lastWord;

        //trim off any brackets e.g. return "My Table" for "[My Table]"
        if (lastWord.StartsWith(OpenQualifier, StringComparison.Ordinal) && lastWord.EndsWith(CloseQualifier, StringComparison.Ordinal))
            return UnescapeWrappedNameBody(lastWord[1..^1]);

        return lastWord;
    }

    /// <summary>
    /// <para>Removes qualifiers/escape sequences in the suplied <paramref name="name"/>.  This should for example convert MySql double backtick escape sequences fi``sh into singles (fi`sh).</para>
    /// 
    /// <para>Method is only called after a successful detection and stripping of <see cref="OpenQualifier"/> and <see cref="CloseQualifier"/></para>
    /// </summary>
    /// <param name="name">A wrapped name after it has had the opening and closing qualifiers stripped off e.g. "Fi``sh"</param>
    /// <returns>The final runtime name unescaped e.g. "Fi`sh"</returns>
    protected virtual string UnescapeWrappedNameBody(string name) => name;

    public virtual bool TryGetRuntimeName(string s, out string? name)
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

    public abstract bool SupportsEmbeddedParameters();

    public string? EnsureWrapped(string? databaseOrTableName)
    {
        if (string.IsNullOrWhiteSpace(databaseOrTableName))
            return databaseOrTableName;

        if (databaseOrTableName.Contains(DatabaseTableSeparator))
            throw new Exception(string.Format(FAnsiStrings.QuerySyntaxHelper_EnsureWrapped_String_passed_to_EnsureWrapped___0___contained_separators__not_allowed____Prohibited_Separator_is___1__, databaseOrTableName, DatabaseTableSeparator));

        return EnsureWrappedImpl(databaseOrTableName);
    }

    public abstract string EnsureWrappedImpl(string databaseOrTableName);

    public abstract string EnsureFullyQualified(string? databaseName, string? schema, string tableName);

    public virtual string EnsureFullyQualified(string? databaseName, string? schema, string tableName, string columnName, bool isTableValuedFunction = false) =>
        isTableValuedFunction ? $"{GetRuntimeName(tableName)}.{GetRuntimeName(columnName)}"
            : //table valued functions do not support database name being in the column level selection list area of sql queries
            $"{EnsureFullyQualified(databaseName, schema, tableName)}.{EnsureWrapped(GetRuntimeName(columnName))}";

    public virtual string Escape(string sql) => string.IsNullOrWhiteSpace(sql) ? sql : sql.Replace("'", "''");
    public abstract TopXResponse HowDoWeAchieveTopX(int x);

    public virtual string GetParameterDeclaration(string proposedNewParameterName, DatabaseTypeRequest request) => GetParameterDeclaration(proposedNewParameterName, TypeTranslater.GetSQLDBTypeForCSharpType(request));

    public virtual HashSet<string> GetReservedWords() => new(StringComparer.CurrentCultureIgnoreCase);

    public abstract string GetParameterDeclaration(string proposedNewParameterName, string sqlType);

    /// <summary>
    /// Splits the given <paramref name="lineToSplit"/> into
    /// </summary>
    /// <param name="lineToSplit"></param>
    /// <param name="selectSQL"></param>
    /// <param name="alias"></param>
    /// <returns></returns>
    public virtual bool SplitLineIntoSelectSQLAndAlias(string lineToSplit, out string selectSQL, [NotNullWhen(true)] out string? alias)
    {
        //Ths line is expected to be some SELECT sql so remove trailing whitespace and commas etc
        lineToSplit = lineToSplit.TrimEnd(',', ' ', '\n', '\r');

        var matches = AliasRegex().Matches(lineToSplit);

        switch (matches.Count)
        {
            case > 1:
                throw new SyntaxErrorException(string.Format(FAnsiStrings.QuerySyntaxHelper_SplitLineIntoSelectSQLAndAlias_, matches.Count, lineToSplit));
            case 0:
                selectSQL = lineToSplit;
                alias = null;
                return false;
        }

        //match is an unwrapped alias
        var unqualifiedAlias = matches[0].Groups[2].Value;
        var qualifiedAlias = matches[0].Groups[4].Value;

        alias = string.IsNullOrWhiteSpace(unqualifiedAlias) ? qualifiedAlias : unqualifiedAlias;
        alias = alias.Trim();
        selectSQL = lineToSplit[..matches[0].Index].Trim();
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

        if (lineToSplit.Count(static c => c.Equals('(')) != lineToSplit.Count(static c => c.Equals(')')))
            throw new ArgumentException(
                FAnsiStrings
                    .QuerySyntaxHelper_SplitLineIntoOuterMostMethodAndContents_The_number_of_opening_and_closing_parentheses_must_match,
                nameof(lineToSplit));

        var firstBracket = lineToSplit.IndexOf('(');

        if (firstBracket == -1)
            throw new ArgumentException(
                FAnsiStrings
                    .QuerySyntaxHelper_SplitLineIntoOuterMostMethodAndContents_Line_must_contain_at_least_one_pair_of_parentheses,
                nameof(lineToSplit));

        method = lineToSplit[..firstBracket].Trim();

        var lastBracket = lineToSplit.LastIndexOf(')');

        var length = lastBracket - (firstBracket + 1);

        contents = length == 0 ? "" : //it's something like count()
            lineToSplit.Substring(firstBracket + 1, length).Trim();
    }

    public static string MakeHeaderNameSensible(string header)
    {
        if (string.IsNullOrWhiteSpace(header))
            return header;

        //replace anything that isn't a digit, letter or underscore with emptiness (except spaces - these will go but first...)
        //also accept anything above ASCII 256
        var r = HeaderNameCharRegex();

        var adjustedHeader = r.Replace(header, "");

        var sb = new StringBuilder(adjustedHeader);

        //Camel case after spaces
        for (var i = 0; i < sb.Length; i++)
            //if we are looking at a space
            if (sb[i] == ' ' && i + 1 < sb.Length && sb[i + 1] >= 'a' && sb[i + 1] <= 'z') //and there is another character
                //and that character is a lower case letter
                sb[i + 1] = char.ToUpper(sb[i + 1]);

        adjustedHeader = sb.ToString().Replace(" ", "");

        //if it starts with a digit (illegal) put an underscore before it
        if (StartsDigitsRe().IsMatch(adjustedHeader))
            adjustedHeader = $"_{adjustedHeader}";

        return adjustedHeader;
    }

    public string GetSensibleEntityNameFromString(string? potentiallyDodgyName)
    {
        potentiallyDodgyName = GetRuntimeName(potentiallyDodgyName);

        //replace anything that isn't a digit, letter or underscore with underscores
        var r = NotAlphaNumRe();
        var adjustedHeader = r.Replace(potentiallyDodgyName ?? string.Empty, "_");

        //if it starts with a digit (illegal) put an underscore before it
        if (StartsDigitsRe().IsMatch(adjustedHeader))
            adjustedHeader = $"_{adjustedHeader}";

        return adjustedHeader;
    }

    public abstract string GetAutoIncrementKeywordIfAny();
    public abstract Dictionary<string, string> GetSQLFunctionsDictionary();

    public bool IsBasicallyNull(object value)
    {
        if (value is string stringValue)
            return string.IsNullOrWhiteSpace(stringValue);

        return value == null || value == DBNull.Value;
    }

    public virtual bool IsTimeout(Exception exception) =>
        /*
        //todo doesn't work with .net standard 
        var oleE = exception as OleDbException;

        if (oleE != null && oleE.ErrorCode == -2147217871)
            return true;*/
        exception.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase);

    public abstract string HowDoWeAchieveMd5(string selectSql);


    public DbParameter GetParameter(DbParameter p, DiscoveredColumn discoveredColumn, object value, CultureInfo? culture)
    {
        try
        {
            culture ??= CultureInfo.InvariantCulture;

            if (!factories.ContainsKey(culture))
                factories.Add(culture, new TypeDeciderFactory(culture));

            var tt = TypeTranslater;
            p.DbType = tt.GetDbTypeForSQLDBType(discoveredColumn.DataType.SQLType);
            var cSharpType = tt.GetCSharpTypeForSQLDBType(discoveredColumn.DataType.SQLType);

            if (IsBasicallyNull(value))
                p.Value = DBNull.Value;
            else if (value is string strVal && factories[culture].IsSupported(cSharpType)) //if the input is a string and it's for a hard type e.g. TimeSpan
            {
                var decider = factories[culture].Create(cSharpType);
                var o = decider.Parse(strVal);

                if (o is DateTime d) o = FormatDateTimeForDbParameter(d);

                //Not all DBMS support DBParameter.Value = new TimeSpan(...);
                if (o is TimeSpan t) o = FormatTimespanForDbParameter(t);


                p.Value = o;
            }
            else
                p.Value = value;
        }
        catch (Exception ex)
        {
            throw new Exception(string.Format(FAnsiStrings.QuerySyntaxHelper_GetParameter_Could_not_GetParameter_for_column___0__, discoveredColumn.GetFullyQualifiedName()), ex);
        }

        return p;
    }

    public void ValidateDatabaseName(string? databaseName)
    {
        if (!IsValidDatabaseName(databaseName, out var reason))
            throw new RuntimeNameException(reason);
    }
    public void ValidateTableName(string tableName)
    {
        if (!IsValidTableName(tableName, out var reason))
            throw new RuntimeNameException(reason);
    }
    public void ValidateColumnName(string columnName)
    {
        if (!IsValidColumnName(columnName, out var reason))
            throw new RuntimeNameException(reason);
    }

    public bool IsValidDatabaseName(string? databaseName, [NotNullWhen(false)] out string? reason)
    {
        reason = ValidateName(databaseName, "Database", MaximumDatabaseLength);
        return string.IsNullOrWhiteSpace(reason);
    }

    public bool IsValidTableName(string tableName, [NotNullWhen(false)] out string? reason)
    {
        reason = ValidateName(tableName, "Table", MaximumTableLength);
        return string.IsNullOrWhiteSpace(reason);
    }

    public bool IsValidColumnName(string columnName, [NotNullWhen(false)] out string? reason)
    {
        reason = ValidateName(columnName, "Column", MaximumColumnLength);
        return string.IsNullOrWhiteSpace(reason);
    }

    public virtual string? GetDefaultSchemaIfAny() => null;

    /// <summary>
    /// returns null if the name is valid.  Otherwise a string describing why it is invalid.
    /// </summary>
    /// <param name="candidate"></param>
    /// <param name="objectType">Type of object being validated e.g. "Database", "Table" etc</param>
    /// <param name="maximumLengthAllowed"></param>
    /// <returns></returns>
    private string? ValidateName(string? candidate, string objectType, int maximumLengthAllowed)
    {
        if (string.IsNullOrWhiteSpace(candidate))
            return string.Format(FAnsiStrings.QuerySyntaxHelper_ValidateName__0__name_cannot_be_blank, objectType);

        if (candidate.Length > maximumLengthAllowed)
            return string.Format(FAnsiStrings.QuerySyntaxHelper_ValidateName__0__name___1___is_too_long_for_the_DBMS___2__supports_maximum_length_of__3__,
                objectType, candidate[..maximumLengthAllowed], DatabaseType, maximumLengthAllowed);

        if (candidate.IndexOfAny(IllegalNameChars) != -1)
            return string.Format(
                FAnsiStrings.QuerySyntaxHelper_ValidateName__0__name___1___contained_unsupported__by_FAnsi__characters___Unsupported_characters_are__2_,
                objectType, candidate, new string(IllegalNameChars));

        return null;
    }


    public DbParameter GetParameter(DbParameter p, DiscoveredColumn discoveredColumn, object value) => GetParameter(p, discoveredColumn, value, null);

    /// <summary>
    /// <para>
    /// Return the appropriate value such that it can be put into a DbParameter.Value field and be succesfully inserted into a
    /// column in the database designed to represent datetime fields (without date).
    /// </para>
    /// <para>Default behaviour is to return unaltered but some DBMS require alterations e.g. UTC tinkering</para>
    /// </summary>
    /// <param name="dateTime"></param>
    /// <returns></returns>
    protected virtual object FormatDateTimeForDbParameter(DateTime dateTime) => dateTime;

    /// <summary>
    /// Return the appropriate value such that it can be put into a DbParameter.Value field and be succesfully inserted into a
    /// column in the database designed to represent time fields (without date).
    /// </summary>
    /// <param name="timeSpan"></param>
    /// <returns></returns>
    protected virtual object FormatTimespanForDbParameter(TimeSpan timeSpan) => timeSpan;

    #region Equality Members
    protected bool Equals(QuerySyntaxHelper other)
    {
        if (other == null)
            return false;

        return GetType() == other.GetType();
    }

    public override bool Equals(object? obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;

        return Equals((QuerySyntaxHelper)obj);
    }

    public override int GetHashCode() => GetType().GetHashCode();

    #endregion

    public Dictionary<T, string> GetParameterNamesFor<T>(T[] columns, Func<T, string?> toStringFunc) where T : notnull
    {
        var toReturn = new Dictionary<T, string>();

        var reservedKeywords = GetReservedWords();


        //sensible parameter names have no spaces or symbols!
        var sensibleParameterNamesInclude = sensibleParameterNamesIncludeRe();

        for (var i = 0; i < columns.Length; i++)
        {
            var c = columns[i];
            var columnName = toStringFunc(c);

            if (columnName is null || !sensibleParameterNamesInclude.IsMatch(columnName)) //if column name is "_:_" or something
                toReturn.Add(c, $"{ParameterSymbol}p{i}");
            else
                toReturn.Add(c, ParameterSymbol + (reservedKeywords.Contains(columnName) ? $"{columnName}1" : columnName)); //if column is reserved keyword or normal name
        }

        return toReturn;
    }

    [GeneratedRegex(@"^\w*$")]
    private static partial Regex sensibleParameterNamesIncludeRe();

    //whitespace followed by as and more whitespace
    //Then any word (optionally bounded by a table name qualifier)
    //alias is a word
    //(w+)
    //alias is a wrapped word e.g. [hey hey].  In this case we must allow anything between the brackets that is not closing bracket
    //[[`""]([^[`""]+)[]`""]
    [GeneratedRegex("""\s+as\s+((\w+)|([[`"]([^[`"]+)[]`"]))$""", RegexOptions.IgnoreCase, "en-US")]
    private static partial Regex AliasRegex();

    [GeneratedRegex("([@:][A-Za-z0-9_]*)\\s?", RegexOptions.IgnoreCase, "en-US")]
    private static partial Regex ParameterNamesRe();

    [GeneratedRegex("[^A-Za-z0-9_ \u0101-\uFFFF]")]
    private static partial Regex HeaderNameCharRegex();

    [GeneratedRegex("^[0-9]")]
    private static partial Regex StartsDigitsRe();

    [GeneratedRegex("[^A-Za-z0-9_]")]
    private static partial Regex NotAlphaNumRe();
}