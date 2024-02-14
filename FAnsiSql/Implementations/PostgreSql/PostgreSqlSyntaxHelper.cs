using System;
using System.Collections.Generic;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.PostgreSql.Aggregation;
using FAnsi.Implementations.PostgreSql.Update;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlSyntaxHelper : QuerySyntaxHelper
{
    public static readonly PostgreSqlSyntaxHelper Instance = new();
    private PostgreSqlSyntaxHelper() : base(PostgreSqlTypeTranslater.Instance,PostgreSqlAggregateHelper.Instance,PostgreSqlUpdateHelper.Instance, DatabaseType.PostgreSql)
    {
    }

    public override int MaximumDatabaseLength => 63;
    public override int MaximumTableLength => 63;
    public override int MaximumColumnLength => 63;

    public const string DefaultPostgresSchema = "public";

    public override string OpenQualifier => "\"";

    public override string CloseQualifier => "\"";

    public override bool SupportsEmbeddedParameters() => false;

    protected override object FormatDateTimeForDbParameter(DateTime dateTime) =>
        // Starting with 4.0.0 npgsql crashes if it has to read a DateTime Unspecified Kind
        // See : https://github.com/npgsql/efcore.pg/issues/2000
        // Also it doesn't support DateTime.Kind.Local
        dateTime.Kind == DateTimeKind.Unspecified ? dateTime.ToUniversalTime():dateTime;

    public override string EnsureWrappedImpl(string databaseOrTableName) => $"\"{GetRuntimeNameWithDoubledDoubleQuotes(databaseOrTableName)}\"";

    /// <summary>
    /// Returns the runtime name of the string with all double quotes escaped (but resulting string is not wrapped itself)
    /// </summary>
    /// <param name="s"></param>
    /// <returns></returns>
    private string GetRuntimeNameWithDoubledDoubleQuotes(string s) => GetRuntimeName(s)?.Replace("\"","\"\"");

    protected override string UnescapeWrappedNameBody(string name) => name.Replace("\"\"","\"");

    public override string EnsureFullyQualified(string databaseName, string schema, string tableName)
    {
        //if there is no schema address it as db..table (which is the same as db.dbo.table in Microsoft SQL Server)
        if(string.IsNullOrWhiteSpace(schema))
            return EnsureWrapped(databaseName) + DatabaseTableSeparator + DefaultPostgresSchema + DatabaseTableSeparator + EnsureWrapped(tableName);

        //there is a schema so add it in
        return EnsureWrapped(databaseName) + DatabaseTableSeparator + EnsureWrapped(schema) + DatabaseTableSeparator + EnsureWrapped(tableName);
    }

    public override string EnsureFullyQualified(string databaseName, string schema, string tableName, string columnName,
        bool isTableValuedFunction = false)
    {
        if (isTableValuedFunction)
            return $"{EnsureWrapped(tableName)}.{EnsureWrapped(GetRuntimeName(columnName))}";//table valued functions do not support database name being in the column level selection list area of sql queries

        return $"{EnsureFullyQualified(databaseName, schema, tableName)}.\"{GetRuntimeName(columnName)}\"";
    }


    public override TopXResponse HowDoWeAchieveTopX(int x) => new($"fetch first {x} rows only", QueryComponent.Postfix);

    public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType) => throw new NotSupportedException();

    public override string GetScalarFunctionSql(MandatoryScalarFunctions function) =>
        function switch
        {
            MandatoryScalarFunctions.GetTodaysDate => "now()",
            MandatoryScalarFunctions.GetGuid => "gen_random_uuid()" //requires pgcrypto e.g. CREATE EXTENSION pgcrypto;
            ,
            MandatoryScalarFunctions.Len => "LENGTH",
            _ => throw new ArgumentOutOfRangeException(nameof(function))
        };

    public override string GetAutoIncrementKeywordIfAny() => "GENERATED ALWAYS AS IDENTITY";

    public override Dictionary<string, string> GetSQLFunctionsDictionary() => [];

    public override string HowDoWeAchieveMd5(string selectSql) => $"MD5({selectSql})";

    public override string GetDefaultSchemaIfAny() => "public";
}