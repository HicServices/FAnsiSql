using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using TypeGuesser;

namespace FAnsi.Implementations.Oracle;

public sealed partial class OracleTypeTranslater:TypeTranslater
{
    public static readonly OracleTypeTranslater Instance = new();
    private static readonly Regex AlsoFloatingPointRegex = AlsoFloatingPointRegexImpl();
    private static readonly Regex AlsoByteArrayRegex = AlsoByteArrayRegexImpl();

    public const int ExtraLengthPerNonAsciiCharacter = 3;

    /// <summary>
    /// Oracle specific string types, these are all max length as returned by <see cref="GetLengthIfString"/>
    /// </summary>
    private static readonly Regex AlsoStringRegex = AlsoStringRegexImpl();


    private OracleTypeTranslater() : base(DateRegexImpl(), 4000, 4000)
    {
    }

    protected override string GetStringDataTypeImpl(int maxExpectedStringWidth) => $"varchar2({maxExpectedStringWidth})";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) => $"nvarchar2({maxExpectedStringWidth})";

    public override string GetStringDataTypeWithUnlimitedWidth() => "CLOB";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "NCLOB";

    protected override string GetTimeDataType() => "TIMESTAMP";

    /// <summary>
    /// <para>Oracle doesn't have a native bit type.  You can only approximate it with char(1) or number(1) and optionally an independent named CHECK constraint if necessary to enforce it</para>
    /// <para>See https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns</para>
    /// </summary>
    /// <returns></returns>
    protected override string GetBoolDataType() => "number(1)";

    protected override string GetSmallIntDataType() => "number(5)";
    protected override string GetIntDataType() => "number(10)";
    protected override string GetBigIntDataType() => "number(19)";

    /// <summary>
    /// <para>Oracle doesn't have a bit character type.  You can only approximate it with char(1) or number(1)</para>
    /// <para>See https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns</para>
    /// </summary>
    /// <param name="sqlType"></param>
    /// <returns></returns>
    protected override bool IsBit(string sqlType) => sqlType.Equals("decimal(1,0)",StringComparison.InvariantCultureIgnoreCase);

    protected override bool IsString(string sqlType) =>
        !sqlType.Contains("RAW", StringComparison.InvariantCultureIgnoreCase) &&
        (base.IsString(sqlType) || AlsoStringRegex.IsMatch(sqlType));

    protected override bool IsFloatingPoint(string sqlType) => base.IsFloatingPoint(sqlType) || AlsoFloatingPointRegex.IsMatch(sqlType);

    public override int GetLengthIfString(string sqlType) => AlsoStringRegex.IsMatch(sqlType) ? int.MaxValue : base.GetLengthIfString(sqlType);

    protected override bool IsSmallInt(string sqlType) =>
        //yup you ask for one of these, you will get a NUMBER(38) https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm
        sqlType.Equals("decimal(5,0)", StringComparison.InvariantCultureIgnoreCase) ||
        (!sqlType.StartsWith("SMALLINT", StringComparison.InvariantCultureIgnoreCase) && base.IsSmallInt(sqlType));

    protected override bool IsInt(string sqlType) =>
        //yup you ask for one of these, you will get a NUMBER(38) https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm
        sqlType.Equals("decimal(10,0)",StringComparison.InvariantCultureIgnoreCase) || sqlType.StartsWith("SMALLINT", StringComparison.InvariantCultureIgnoreCase) || base.IsInt(sqlType);

    protected override bool IsLong(string sqlType) => sqlType.Equals("decimal(19,0)", StringComparison.InvariantCultureIgnoreCase) || base.IsLong(sqlType);
    protected override bool IsByteArray(string sqlType) => base.IsByteArray(sqlType) || AlsoByteArrayRegex.IsMatch(sqlType);

    protected override string GetDateDateTimeDataType() => "DATE";

    public override Guesser GetGuesserFor(DiscoveredColumn discoveredColumn) => base.GetGuesserFor(discoveredColumn, ExtraLengthPerNonAsciiCharacter);

    [GeneratedRegex("^([N]?CLOB)|(LONG)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoStringRegexImpl();
    [GeneratedRegex("^(NUMBER)|(DEC)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoFloatingPointRegexImpl();
    [GeneratedRegex("(BFILE)|(BLOB)|(RAW)|(ROWID)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoByteArrayRegexImpl();
    [GeneratedRegex("(date)|(timestamp)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRegexImpl();
}