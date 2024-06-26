﻿using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;
using NpgsqlTypes;

namespace FAnsi.Implementations.PostgreSql;

public sealed partial class PostgreSqlTypeTranslater : TypeTranslater
{
    public static readonly PostgreSqlTypeTranslater Instance = new();

    private PostgreSqlTypeTranslater() : base(DateRegexImpl(), 8000, 4000)
    {
        TimeRegex = TimeRegexImpl(); //space is important
    }

    public override string GetStringDataTypeWithUnlimitedWidth() => "text";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) => GetStringDataType(maxExpectedStringWidth);

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "text";

    protected override string GetDateDateTimeDataType() => "timestamp";

    public NpgsqlDbType GetNpgsqlDbTypeForCSharpType(Type t)
    {

        if (t == typeof(bool) || t == typeof(bool?))
            return NpgsqlDbType.Boolean;

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

    [GeneratedRegex("timestamp", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRegexImpl();
    [GeneratedRegex("^time ", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex TimeRegexImpl();
}