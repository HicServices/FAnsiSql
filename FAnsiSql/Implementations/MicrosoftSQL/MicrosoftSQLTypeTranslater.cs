using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed partial class MicrosoftSQLTypeTranslater : TypeTranslater
{
    public static readonly MicrosoftSQLTypeTranslater Instance = new();

    private static readonly Regex AlsoBinaryRegex = AlsoBinaryRe();

    private MicrosoftSQLTypeTranslater() : base(DateRe(), 8000, 4000)
    {
    }

    protected override string GetDateDateTimeDataType() => "datetime2";

    public override string GetStringDataTypeWithUnlimitedWidth() => "varchar(max)";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "nvarchar(max)";

    protected override bool IsByteArray(string sqlType) => base.IsByteArray(sqlType) || AlsoBinaryRegex.IsMatch(sqlType);
    [GeneratedRegex("date", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRe();
    [GeneratedRegex("(image)|(timestamp)|(rowversion)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoBinaryRe();
}