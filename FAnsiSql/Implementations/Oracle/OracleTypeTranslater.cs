﻿using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using TypeGuesser;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleTypeTranslater:TypeTranslater
{
    public static readonly OracleTypeTranslater Instance = new();
    private static readonly Regex AlsoFloatingPointRegex = new("^(NUMBER)|(DEC)",RegexOptions.CultureInvariant|RegexOptions.IgnoreCase|RegexOptions.Compiled);
    private static readonly Regex AlsoByteArrayRegex = new("(BFILE)|(BLOB)|(RAW)|(ROWID)", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
        
    public const int ExtraLengthPerNonAsciiCharacter = 3;

    /// <summary>
    /// Oracle specific string types, these are all max length as returned by <see cref="GetLengthIfString"/>
    /// </summary>
    private static readonly Regex AlsoStringRegex = new("^([N]?CLOB)|(LONG)", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
        

    private OracleTypeTranslater(): base(4000, 4000)
    {
        DateRegex = new Regex("(date)|(timestamp)", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
    }
    protected override string GetStringDataTypeImpl(int maxExpectedStringWidth) => $"varchar2({maxExpectedStringWidth})";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) => $"nvarchar2({maxExpectedStringWidth})";

    public override string GetStringDataTypeWithUnlimitedWidth() => "CLOB";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "NCLOB";

    protected override string GetTimeDataType() => "TIMESTAMP";

    /// <summary>
    /// <para>Returns char(5).  Oracle doesn't have a bit character type.  You can only approximate it with char(1) or number(1) and an independent named CHECK constraint
    /// For our purposes we will have to just use varchar2(5) to store "True" or "False" and "1" and "0" etc</para>
    /// 
    /// <para>See https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns</para>
    /// </summary>
    /// <returns></returns>
    protected override string GetBoolDataType() => "varchar2(5)";

    protected override string GetBigIntDataType() => "long";

    /// <summary>
    /// <para>Returns False.  Oracle doesn't have a bit character type.  You can only approximate it with char(1) or number(1) and an independent named CHECK constraint
    /// For our purposes we will have to just use varchar2(5) to store "True" or "False" and "1" and "0" etc</para>
    /// 
    /// <para>See https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns</para>
    /// </summary>
    /// <param name="sqlType"></param>
    /// <returns></returns>
    protected override bool IsBit(string sqlType) => false;

    protected override bool IsString(string sqlType)
    {
        return !sqlType.Contains("RAW", StringComparison.InvariantCultureIgnoreCase) &&
               (base.IsString(sqlType) || AlsoStringRegex.IsMatch(sqlType));
    }

    protected override bool IsFloatingPoint(string sqlType) => base.IsFloatingPoint(sqlType) || AlsoFloatingPointRegex.IsMatch(sqlType);

    public override int GetLengthIfString(string sqlType) => AlsoStringRegex.IsMatch(sqlType) ? int.MaxValue : base.GetLengthIfString(sqlType);

    protected override bool IsSmallInt(string sqlType) =>
        //yup you ask for one of these, you will get a NUMBER(38) https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm
        !sqlType.StartsWith("SMALLINT", StringComparison.InvariantCultureIgnoreCase) && base.IsSmallInt(sqlType);

    protected override bool IsInt(string sqlType) =>
        //yup you ask for one of these, you will get a NUMBER(38) https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm
        sqlType.StartsWith("SMALLINT", StringComparison.CurrentCultureIgnoreCase) || base.IsInt(sqlType);

    protected override bool IsByteArray(string sqlType) => base.IsByteArray(sqlType) || AlsoByteArrayRegex.IsMatch(sqlType);

    protected override string GetDateDateTimeDataType() => "DATE";

    public override Guesser GetGuesserFor(DiscoveredColumn discoveredColumn)
    {
        return base.GetGuesserFor(discoveredColumn, ExtraLengthPerNonAsciiCharacter);
    }
}