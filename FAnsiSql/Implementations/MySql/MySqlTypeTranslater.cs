using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Implementations.MySql;

public sealed partial class MySqlTypeTranslater : TypeTranslater
{
    public static readonly MySqlTypeTranslater Instance = new();

    //yup that's right!, long is string (MEDIUMTEXT)
    //https://dev.mysql.com/doc/refman/8.0/en/other-vendor-data-types.html
    private static readonly Regex AlsoBitRegex = AlsoBitRe();
    private static readonly Regex AlsoStringRegex = AlsoStringRe();
    private static readonly Regex AlsoFloatingPoint = AlsoFloatingPointRe();

    private MySqlTypeTranslater() : base(4000, 4000)
    {
        //match bigint and bigint(20) etc
        ByteRegex = ByteRe();
        SmallIntRegex = SmallIntRe();
        IntRegex = IntRe();
        LongRegex = LongRe();
        DateRegex = DateRe();
    }

    public override int GetLengthIfString(string sqlType) =>
        sqlType.ToUpperInvariant() switch
        {
            "TINYTEXT" => 1 << 8,
            "TEXT" => 1 << 16,
            "MEDIUMTEXT" => 1 << 24,
            "LONGTEXT" => int.MaxValue, // Should be 1<<32 but that overflows...
            _ => AlsoStringRegex.IsMatch(sqlType) ? int.MaxValue : base.GetLengthIfString(sqlType)
        };

    public override string GetStringDataTypeWithUnlimitedWidth()
    {
        return "longtext";
    }

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth()
    {
        return "longtext";
    }

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth)
    {
        return $"varchar({maxExpectedStringWidth})";
    }

    protected override bool IsInt(string sqlType)
    {
        //not an int
        return !sqlType.StartsWith("int8", StringComparison.InvariantCultureIgnoreCase) && base.IsInt(sqlType);
    }

    protected override bool IsString(string sqlType)
    {
        if (sqlType.Contains("binary",StringComparison.InvariantCultureIgnoreCase))
            return false;

        return base.IsString(sqlType) || AlsoStringRegex.IsMatch(sqlType);
    }

    protected override bool IsFloatingPoint(string sqlType)
    {
        return base.IsFloatingPoint(sqlType) || AlsoFloatingPoint.IsMatch(sqlType);
    }

    protected override bool IsBit(string sqlType)
    {
        return base.IsBit(sqlType) || AlsoBitRegex.IsMatch(sqlType);
    }

    [GeneratedRegex(@"tinyint\(1\)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoBitRe();
    [GeneratedRegex("(long)|(enum)|(set)|(text)|(mediumtext)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoStringRe();
    [GeneratedRegex(@"^(tinyint)|(int1)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex ByteRe();
    [GeneratedRegex("^(dec)|(fixed)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoFloatingPointRe();
    [GeneratedRegex(@"^(smallint)|(int2)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex SmallIntRe();
    [GeneratedRegex(@"^(int)|(mediumint)|(middleint)|(int3)|(int4)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex IntRe();
    [GeneratedRegex(@"^(bigint)|(int8)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex LongRe();
    [GeneratedRegex(@"(date)|(timestamp)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRe();
}