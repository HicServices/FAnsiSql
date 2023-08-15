using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Implementations.MySql;

public sealed class MySqlTypeTranslater : TypeTranslater
{
    public static readonly MySqlTypeTranslater Instance = new();

    //yup that's right!, long is string (MEDIUMTEXT)
    //https://dev.mysql.com/doc/refman/8.0/en/other-vendor-data-types.html
    private static readonly Regex AlsoBitRegex = new(@"tinyint\(1\)",RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
    private static readonly Regex AlsoStringRegex = new("(long)|(enum)|(set)|(text)|(mediumtext)",RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
    private static readonly Regex AlsoFloatingPoint = new("^(dec)|(fixed)",RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);

    private MySqlTypeTranslater() : base(4000, 4000)
    {
        //match bigint and bigint(20) etc
        ByteRegex = new Regex(@"^(tinyint)|(int1)",RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
        SmallIntRegex = new Regex(@"^(smallint)|(int2)", RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
        IntRegex = new Regex(@"^(int)|(mediumint)|(middleint)|(int3)|(int4)",RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
        LongRegex = new Regex(@"^(bigint)|(int8)", RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
        DateRegex = new Regex(@"(date)|(timestamp)",RegexOptions.IgnoreCase|RegexOptions.Compiled|RegexOptions.CultureInvariant);
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
}