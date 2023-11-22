using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;
using NpgsqlTypes;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlTypeTranslater : TypeTranslater
{
    public static readonly PostgreSqlTypeTranslater Instance = new();
    private PostgreSqlTypeTranslater() : base(8000, 4000)
    {
        DateRegex = new Regex("timestamp", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);
        TimeRegex = new Regex("^time ", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant); //space is important
    }

    public override string GetStringDataTypeWithUnlimitedWidth()
    {
        return "text";
    }

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth)
    {
        return GetStringDataType(maxExpectedStringWidth);
    }

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth()
    {
        return "text";
    }

    protected override string GetDateDateTimeDataType()
    {
        return "timestamp";
    }

    public NpgsqlDbType GetNpgsqlDbTypeForCSharpType(Type t)
    {

        if (t == typeof(bool) || t == typeof(bool?))
            return NpgsqlDbType.Bit;

        if (t == typeof(byte))
            return NpgsqlDbType.Bytea;

        if (t == typeof(short) || t == typeof(short) || t == typeof(ushort) || t == typeof(short?) || t == typeof(ushort?))
            return NpgsqlDbType.Smallint;

        if (t == typeof(int) || t == typeof(int)  || t == typeof(uint) || t == typeof(int?) || t == typeof(uint?))
            return NpgsqlDbType.Integer;

        if (t == typeof (long) || t == typeof(ulong) || t == typeof(long?) || t == typeof(ulong?))
            return NpgsqlDbType.Bigint;

        if (t == typeof(float) || t == typeof(float?) || t == typeof(double) ||
            t == typeof(double?))
            return NpgsqlDbType.Double;

        if (t == typeof(decimal) || t == typeof(decimal?))
            return NpgsqlDbType.Numeric;

        if (t == typeof(string))
            return NpgsqlDbType.Text;

        if (t == typeof(DateTime) || t == typeof(DateTime?))
            return NpgsqlDbType.Timestamp;

        if (t == typeof(TimeSpan) || t == typeof(TimeSpan?))
            return NpgsqlDbType.Time;

        if (t == typeof(Guid))
            return NpgsqlDbType.Uuid;

        throw new TypeNotMappedException(string.Format(FAnsiStrings.TypeTranslater_GetSQLDBTypeForCSharpType_Unsure_what_SQL_type_to_use_for_CSharp_Type___0_____TypeTranslater_was___1__, t.Name, GetType().Name));

    }
}