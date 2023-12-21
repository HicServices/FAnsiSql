using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using TypeGuesser;

namespace FAnsi.Discovery.TypeTranslation;

/// <inheritdoc cref="ITypeTranslater"/>
public abstract partial class TypeTranslater:ITypeTranslater
{
    private const string StringSizeRegexPattern = @"\(([0-9]+)\)";
    private const string DecimalsBeforeAndAfterPattern = @"\(([0-9]+),([0-9]+)\)";

    //Take special note of the use or absence of ^ in the regex to do Contains or StartsWith
    //Ideally don't use $ (end of string) since databases can stick extraneous stuff on the end in many cases

    private static readonly Regex BitRegex = BitRegexImpl();
    protected Regex ByteRegex = ByteRegexImpl();
    protected Regex SmallIntRegex = SmallIntRe();
    protected Regex IntRegex = IntRe();
    protected Regex LongRegex = LongRe();
    protected Regex DateRegex;
    protected Regex TimeRegex = TimeRe();
    private static readonly Regex StringRegex = StringRe();
    private static readonly Regex ByteArrayRegex = ByteArrayRe();
    private static readonly Regex FloatingPointRegex = FloatingPointRe();
    private static readonly Regex GuidRegex = GuidRe();

    /// <summary>
    /// The maximum number of characters to declare explicitly in the char type (e.g. varchar(500)) before instead declaring the text/varchar(max) etc type
    /// appropriate to the database engine being targeted
    /// </summary>
    private readonly int MaxStringWidthBeforeMax;

    /// <summary>
    /// The size to declare string fields when the API user has neglected to supply a length.  This should be high, if you want to avoid lots of extra long columns
    /// use <see cref="Guesser"/> to determine the required length/type at runtime.
    /// </summary>
    private readonly int StringWidthWhenNotSupplied;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="maxStringWidthBeforeMax"><see cref="MaxStringWidthBeforeMax"/></param>
    /// <param name="stringWidthWhenNotSupplied"><see cref="StringWidthWhenNotSupplied"/></param>
    protected TypeTranslater(int maxStringWidthBeforeMax, int stringWidthWhenNotSupplied)
    {
        MaxStringWidthBeforeMax = maxStringWidthBeforeMax;
        StringWidthWhenNotSupplied = stringWidthWhenNotSupplied;
    }

    public string GetSQLDBTypeForCSharpType(DatabaseTypeRequest request)
    {
        var t = request.CSharpType;

        if (t == typeof(bool) || t == typeof(bool?))
            return GetBoolDataType();

        if (t == typeof(byte))
            return GetByteDataType();

        if (t == typeof(short) || t == typeof(short) || t == typeof(ushort) || t == typeof(short?) || t == typeof(ushort?))
            return GetSmallIntDataType();

        if (t == typeof(int) || t == typeof(int)  || t == typeof(uint) || t == typeof(int?) || t == typeof(uint?))
            return GetIntDataType();

        if (t == typeof (long) || t == typeof(ulong) || t == typeof(long?) || t == typeof(ulong?))
            return GetBigIntDataType();

        if (t == typeof(float) || t == typeof(float?) || t == typeof(double) ||
            t == typeof(double?) || t == typeof(decimal) ||
            t == typeof(decimal?))
            return GetFloatingPointDataType(request.Size);

        if (t == typeof(string)) return request.Unicode ? GetUnicodeStringDataType(request.Width) : GetStringDataType(request.Width);

        if (t == typeof(DateTime) || t == typeof(DateTime?))
            return GetDateDateTimeDataType();

        if (t == typeof(TimeSpan) || t == typeof(TimeSpan?))
            return GetTimeDataType();

        if (t == typeof (byte[]))
            return GetByteArrayDataType();

        if (t == typeof (Guid))
            return GetGuidDataType();

        throw new TypeNotMappedException(string.Format(FAnsiStrings.TypeTranslater_GetSQLDBTypeForCSharpType_Unsure_what_SQL_type_to_use_for_CSharp_Type___0_____TypeTranslater_was___1__, t.Name, GetType().Name));
    }

    private static string GetByteArrayDataType()
    {
        return "varbinary(max)";
    }

    private static string GetByteDataType()
    {
        return "tinyint";
    }

    protected string GetFloatingPointDataType(DecimalSize decimalSize)
    {
        if (decimalSize == null || decimalSize.IsEmpty)
            return "decimal(20,10)";

        return $"decimal({decimalSize.Precision},{decimalSize.Scale})";
    }

    protected virtual string GetDateDateTimeDataType()
    {
        return "datetime";
    }

    protected string GetStringDataType(int? maxExpectedStringWidth)
    {
        if (maxExpectedStringWidth == null)
            return GetStringDataTypeImpl(StringWidthWhenNotSupplied);

        if (maxExpectedStringWidth > MaxStringWidthBeforeMax)
            return GetStringDataTypeWithUnlimitedWidth();

        return GetStringDataTypeImpl(maxExpectedStringWidth.Value);
    }

    protected virtual string GetStringDataTypeImpl(int maxExpectedStringWidth)
    {
        return $"varchar({maxExpectedStringWidth})";
    }

    public abstract string GetStringDataTypeWithUnlimitedWidth();


    private string GetUnicodeStringDataType(int? maxExpectedStringWidth)
    {
        if (maxExpectedStringWidth == null)
            return GetUnicodeStringDataTypeImpl(StringWidthWhenNotSupplied);

        if (maxExpectedStringWidth > MaxStringWidthBeforeMax)
            return GetUnicodeStringDataTypeWithUnlimitedWidth();

        return GetUnicodeStringDataTypeImpl(maxExpectedStringWidth.Value);
    }

    protected virtual string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth)
    {
        return $"nvarchar({maxExpectedStringWidth})";
    }

    public abstract string GetUnicodeStringDataTypeWithUnlimitedWidth();

    protected virtual string GetTimeDataType()
    {
        return "time";
    }

    protected virtual string GetBoolDataType()
    {
        return "bit";
    }

    protected virtual string GetSmallIntDataType()
    {
        return "smallint";
    }

    protected virtual string GetIntDataType()
    {
        return "int";
    }

    protected virtual string GetBigIntDataType()
    {
        return "bigint";
    }

    protected string GetGuidDataType()
    {
        return "uniqueidentifier";
    }

    /// <inheritdoc/>
    [return:
        DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties |
                                   DynamicallyAccessedMemberTypes.PublicFields)]
    public Type GetCSharpTypeForSQLDBType(string sqlType)
    {
        return TryGetCSharpTypeForSQLDBType(sqlType) ??
            throw new TypeNotMappedException(string.Format(
                FAnsiStrings
                    .TypeTranslater_GetCSharpTypeForSQLDBType_No_CSharp_type_mapping_exists_for_SQL_type___0____TypeTranslater_was___1___,
                sqlType, GetType().Name));
    }

    /// <inheritdoc/>
    [return:
        DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties |
                                   DynamicallyAccessedMemberTypes.PublicFields)]
    public Type TryGetCSharpTypeForSQLDBType(string sqlType)
    {
        if (IsBit(sqlType))
            return typeof(bool);

        if (IsByte(sqlType))
            return typeof(byte);

        if (IsSmallInt(sqlType))
            return typeof(short);

        if (IsInt(sqlType))
            return typeof(int);

        if (IsLong(sqlType))
            return typeof(long);

        if (IsFloatingPoint(sqlType))
            return typeof(decimal);

        if (IsString(sqlType))
            return typeof(string);

        if (IsDate(sqlType))
            return typeof(DateTime);

        if (IsTime(sqlType))
            return typeof(TimeSpan);

        if (IsByteArray(sqlType))
            return typeof(byte[]);

        if (IsGuid(sqlType))
            return typeof(Guid);

        return null;
    }

    /// <inheritdoc/>
    public bool IsSupportedSQLDBType(string sqlType)
    {
        return TryGetCSharpTypeForSQLDBType(sqlType) != null;
    }

    /// <inheritdoc/>
    public DbType GetDbTypeForSQLDBType(string sqlType)
    {

        if (IsBit(sqlType))
            return DbType.Boolean;

        if (IsByte(sqlType))
            return DbType.Byte;

        if (IsSmallInt(sqlType))
            return DbType.Int16;

        if (IsInt(sqlType))
            return DbType.Int32;

        if (IsLong(sqlType))
            return DbType.Int64;

        if (IsFloatingPoint(sqlType))
            return DbType.Decimal;

        if (IsString(sqlType))
            return DbType.String;

        if (IsDate(sqlType))
            return DbType.DateTime;

        if (IsTime(sqlType))
            return DbType.Time;

        if (IsByteArray(sqlType))
            return DbType.Object;

        if (IsGuid(sqlType))
            return DbType.Guid;

        throw new TypeNotMappedException(string.Format(
            FAnsiStrings
                .TypeTranslater_GetCSharpTypeForSQLDBType_No_CSharp_type_mapping_exists_for_SQL_type___0____TypeTranslater_was___1___,
            sqlType, GetType().Name));
    }

    public virtual DatabaseTypeRequest GetDataTypeRequestForSQLDBType(string sqlType)
    {
        var cSharpType = GetCSharpTypeForSQLDBType(sqlType);

        var digits = GetDigitsBeforeAndAfterDecimalPointIfDecimal(sqlType);

        var lengthIfString = GetLengthIfString(sqlType);

        //lengthIfString should still be populated even for digits etc because it might be that we have to fallback from "1.2" which is decimal(2,1) to varchar(3) if we see "F" appearing
        if (digits != null)
            lengthIfString = Math.Max(lengthIfString, digits.ToStringLength());

        if (cSharpType == typeof(DateTime))
            lengthIfString = GetStringLengthForDateTime();

        if (cSharpType == typeof(TimeSpan))
            lengthIfString = GetStringLengthForTimeSpan();

        var request = new DatabaseTypeRequest(cSharpType, lengthIfString, digits);

        if (cSharpType == typeof(string))
            request.Unicode = IsUnicode(sqlType);

        return request;
    }

    /// <summary>
    /// Returns true if the <paramref name="sqlType"/> (proprietary DBMS type) is a unicode string type e.g. "nvarchar".  Otherwise returns false
    /// e.g. "varchar"
    /// </summary>
    /// <param name="sqlType"></param>
    /// <returns></returns>
    public bool IsUnicode(string sqlType)
    {
        return sqlType != null && sqlType.StartsWith("n",StringComparison.CurrentCultureIgnoreCase);
    }

    public virtual Guesser GetGuesserFor(DiscoveredColumn discoveredColumn)
    {
        return GetGuesserFor(discoveredColumn, 0);
    }

    protected Guesser GetGuesserFor(DiscoveredColumn discoveredColumn, int extraLengthPerNonAsciiCharacter)
    {
        var reqType = GetDataTypeRequestForSQLDBType(discoveredColumn.DataType.SQLType);
        return new Guesser(reqType)
        {
            ExtraLengthPerNonAsciiCharacter = extraLengthPerNonAsciiCharacter
        };
    }

    public virtual int GetLengthIfString(string sqlType)
    {
        if (string.IsNullOrWhiteSpace(sqlType))
            return -1;

        if (sqlType.Contains("(max)", StringComparison.OrdinalIgnoreCase) || sqlType.ToLower().Equals("text") || sqlType.ToLower().Equals("ntext"))
            return int.MaxValue;

        if (sqlType.Contains("char", StringComparison.OrdinalIgnoreCase))
        {
            var match = StringSizeRegex().Match(sqlType);
            if (match.Success)
                return int.Parse(match.Groups[1].Value);
        }

        return -1;
    }

    public DecimalSize GetDigitsBeforeAndAfterDecimalPointIfDecimal(string sqlType)
    {
        if (string.IsNullOrWhiteSpace(sqlType))
            return null;

        var match = DecimalsBeforeAndAfterRe().Match(sqlType);
        if (!match.Success) return null;

        var precision = int.Parse(match.Groups[1].Value);
        var scale = int.Parse(match.Groups[2].Value);
        return new DecimalSize(precision - scale, scale);
    }

    public string TranslateSQLDBType(string sqlType, ITypeTranslater destinationTypeTranslater)
    {
        //e.g. data_type is datetime2 (i.e. Sql Server), this returns System.DateTime
        var requested = GetDataTypeRequestForSQLDBType(sqlType);

        //this then returns datetime (e.g. mysql)
        return destinationTypeTranslater.GetSQLDBTypeForCSharpType(requested);
    }


    /// <summary>
    /// Return the number of characters required to not truncate/lose any data when altering a column from time (e.g. TIME etc) to varchar(x).  Return
    /// x such that the column does not loose integrity.  This is needed when dynamically discovering what size to make a column by streaming data into a table.
    /// if we see many times and nulls we will decide to use a time column then we see strings and have to convert the column to a varchar column without loosing the
    /// currently loaded data.
    /// </summary>
    /// <returns></returns>
    protected int GetStringLengthForTimeSpan()
    {
        /*
         * 
         * To determine this you can run the following SQL:
          
         create table omgTimes (
dt time 
)

insert into omgTimes values (CONVERT(TIME, GETDATE()))

select * from omgTimes

alter table omgTimes alter column dt varchar(100)

select LEN(dt) from omgTimes
         

         * 
         * */
        return 16; //e.g. "13:10:58.2300000"
    }

    /// <summary>
    /// Return the number of characters required to not truncate/loose any data when altering a column from datetime (e.g. datetime2, DATE etc) to varchar(x).  Return
    /// x such that the column does not lose integrity.  This is needed when dynamically discovering what size to make a column by streaming data into a table.
    /// if we see many dates and nulls we will decide to use a date column then we see strings and have to convert the column to a varchar column without loosing the
    /// currently loaded data.
    /// </summary>
    /// <returns></returns>
    protected int GetStringLengthForDateTime()
    {
        /*
         To determine this you can run the following SQL:

create table omgdates (
dt datetime2 
)

insert into omgdates values (getdate())

select * from omgdates

alter table omgdates alter column dt varchar(100)

select LEN(dt) from omgdates
         */

        return Guesser.MinimumLengthRequiredForDateStringRepresentation; //e.g. "2018-01-30 13:05:45.1266667"
    }

    protected virtual bool IsBit(string sqlType)
    {
        return BitRegex.IsMatch(sqlType);
    }
    protected bool IsByte(string sqlType)
    {
        return ByteRegex.IsMatch(sqlType);
    }
    protected virtual bool IsSmallInt(string sqlType)
    {
        return SmallIntRegex.IsMatch(sqlType);
    }
    protected virtual bool IsInt(string sqlType)
    {
        return IntRegex.IsMatch(sqlType);
    }
    protected virtual bool IsLong(string sqlType)
    {
        return LongRegex.IsMatch(sqlType);
    }
    protected bool IsDate(string sqlType)
    {
        return DateRegex.IsMatch(sqlType);
    }
    protected bool IsTime(string sqlType)
    {
        return TimeRegex.IsMatch(sqlType);
    }
    protected virtual bool IsString(string sqlType)
    {
        return StringRegex.IsMatch(sqlType);
    }
    protected virtual bool IsByteArray(string sqlType)
    {
        return ByteArrayRegex.IsMatch(sqlType);
    }
    protected virtual bool IsFloatingPoint(string sqlType)
    {
        return FloatingPointRegex.IsMatch(sqlType);
    }
    protected bool IsGuid(string sqlType)
    {
        return GuidRegex.IsMatch(sqlType);
    }

    [GeneratedRegex(StringSizeRegexPattern)]
    private static partial Regex StringSizeRegex();
    [GeneratedRegex("^(bit)|(bool)|(boolean)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex BitRegexImpl();
    [GeneratedRegex("^tinyint", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex ByteRegexImpl();
    [GeneratedRegex("^uniqueidentifier", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex GuidRe();
    [GeneratedRegex("^smallint", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex SmallIntRe();
    [GeneratedRegex("^(int)|(integer)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex IntRe();
    [GeneratedRegex("^bigint", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex LongRe();
    [GeneratedRegex("^time$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex TimeRe();
    [GeneratedRegex("(char)|(text)|(xml)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex StringRe();
    [GeneratedRegex("(binary)|(blob)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex ByteArrayRe();
    [GeneratedRegex("^(float)|(decimal)|(numeric)|(real)|(money)|(smallmoney)|(double)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex FloatingPointRe();
    [GeneratedRegex(DecimalsBeforeAndAfterPattern)]
    private static partial Regex DecimalsBeforeAndAfterRe();
}