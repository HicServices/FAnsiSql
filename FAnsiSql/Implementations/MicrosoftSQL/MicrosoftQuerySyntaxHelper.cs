using System;
using System.Collections.Generic;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.MicrosoftSQL.Aggregation;
using FAnsi.Implementations.MicrosoftSQL.Update;
using Microsoft.Data.SqlClient;

namespace FAnsi.Implementations.MicrosoftSQL;

/// <inheritdoc/>
public class MicrosoftQuerySyntaxHelper : QuerySyntaxHelper
{
    public static readonly MicrosoftQuerySyntaxHelper Instance=new();
    private MicrosoftQuerySyntaxHelper() : base(MicrosoftSQLTypeTranslater.Instance,new MicrosoftSQLAggregateHelper(),new MicrosoftSQLUpdateHelper(),DatabaseType.MicrosoftSQLServer)
    {
    }

    /// <summary>
    /// Maximum database name length.  This is less than 128 in order to allow for "_logs" etc getting appended to end.
    /// See: https://stackoverflow.com/a/5096245/4824531 
    /// </summary>
    public override int MaximumDatabaseLength => 100; 
    public override int MaximumTableLength => 128;
    public override int MaximumColumnLength => 128;

    public override string OpenQualifier => "[";

    public override string CloseQualifier => "]";

    public override TopXResponse HowDoWeAchieveTopX(int x)
    {
        return new TopXResponse($"TOP {x}", QueryComponent.SELECT);
    }

    public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType)
    {
        return $"DECLARE {proposedNewParameterName} AS {sqlType};";
    }

    public override string GetScalarFunctionSql(MandatoryScalarFunctions function) =>
        function switch
        {
            MandatoryScalarFunctions.GetTodaysDate => "GETDATE()",
            MandatoryScalarFunctions.GetGuid => "newid()",
            MandatoryScalarFunctions.Len => "LEN",
            _ => throw new ArgumentOutOfRangeException(nameof(function))
        };

    public override string GetAutoIncrementKeywordIfAny()
    {
        return "IDENTITY(1,1)";
    }

    public override Dictionary<string, string> GetSQLFunctionsDictionary() =>
        new()
        {
            { "left", "LEFT ( character_expression , integer_expression )" },
            { "right", "RIGHT ( character_expression , integer_expression )" },
            { "upper", "UPPER ( character_expression )" },
            { "substring","SUBSTRING ( expression ,start , length ) "},
            { "dateadd","DATEADD (datepart , number , date )"},
            { "datediff", "DATEDIFF ( datepart , startdate , enddate )  "},
            { "getdate", "GETDATE()"},
            { "cast", "CAST ( expression AS data_type [ ( length ) ] )"},
            { "convert","CONVERT ( data_type [ ( length ) ] , expression [ , style ] ) "},
            { "case","CASE WHEN x=y THEN 'something' WHEN x=z THEN 'something2' ELSE 'something3' END"}
        };

    public override bool IsTimeout(Exception exception)
    {
        if (exception is SqlException sqlE)
        {
            if (sqlE.Number is -2 or 11 or 1205)
                return true;

            //yup, I've seen this behaviour from Sql Server.  ExceptionMessage of " " and .Number of 
            if (string.IsNullOrWhiteSpace(sqlE.Message) && sqlE.Number == 3617)
                return true;
        }

        return base.IsTimeout(exception);
    }

    public override string HowDoWeAchieveMd5(string selectSql)
    {
        return $"CONVERT(NVARCHAR(32),HASHBYTES('MD5', CONVERT(varbinary,{selectSql})),2)";
    }

    public override string GetDefaultSchemaIfAny()
    {
        return "dbo";
    }

    public override bool SupportsEmbeddedParameters()
    {
        return true;
    }
        
    public override string EnsureWrappedImpl(string databaseOrTableName)
    {
        return $"[{GetRuntimeNameWithDoubledClosingSquareBrackets(databaseOrTableName)}]";
    }

        
    protected override string UnescapeWrappedNameBody(string name)
    {
        return name.Replace("]]","]");
    }

    /// <summary>
    /// Returns the runtime name of the string with all ending square brackets escaped by doubling up (but resulting string is not wrapped itself)
    /// </summary>
    /// <param name="s"></param>
    /// <returns></returns>
    private string GetRuntimeNameWithDoubledClosingSquareBrackets(string s)
    {
        return GetRuntimeName(s)?.Replace("]","]]");
    }

    public override string EnsureFullyQualified(string databaseName, string schema, string tableName)
    {
        //if there is no schema address it as db..table (which is the same as db.dbo.table in Microsoft SQL Server)
        if(string.IsNullOrWhiteSpace(schema))
            return EnsureWrapped( GetRuntimeName(databaseName)) + DatabaseTableSeparator + DatabaseTableSeparator + EnsureWrapped(GetRuntimeName(tableName));

        //there is a schema so add it in
        return EnsureWrapped(GetRuntimeName(databaseName)) + DatabaseTableSeparator + EnsureWrapped(GetRuntimeName(schema)) + DatabaseTableSeparator + EnsureWrapped( GetRuntimeName(tableName));
    }

    public override string EnsureFullyQualified(string databaseName, string schema, string tableName, string columnName, bool isTableValuedFunction = false)
    {
        if (isTableValuedFunction)
            return GetRuntimeName(tableName) + DatabaseTableSeparator + EnsureWrapped(GetRuntimeName(columnName));//table valued functions do not support database name being in the column level selection list area of sql queries

        return EnsureFullyQualified(databaseName,schema,tableName) + DatabaseTableSeparator + EnsureWrapped(GetRuntimeName(columnName));
    }
}