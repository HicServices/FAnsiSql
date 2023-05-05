using System;
using System.Collections.Generic;
using System.Text;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.MySql.Aggregation;
using FAnsi.Implementations.MySql.Update;

namespace FAnsi.Implementations.MySql;

public class MySqlQuerySyntaxHelper : QuerySyntaxHelper
{
    public static readonly MySqlQuerySyntaxHelper Instance = new();
    public override int MaximumDatabaseLength => 64;
    public override int MaximumTableLength => 64;
    public override int MaximumColumnLength => 64;


        
    public override string OpenQualifier => "`";

    public override string CloseQualifier => "`";

    private MySqlQuerySyntaxHelper() : base(MySqlTypeTranslater.Instance, MySqlAggregateHelper.Instance,MySqlUpdateHelper.Instance,DatabaseType.MySql)//no specific type translation required
    {
    }

    public override bool SupportsEmbeddedParameters() => true;

    public override string EnsureWrappedImpl(string databaseOrTableName) => $"`{GetRuntimeNameWithDoubledBackticks(databaseOrTableName)}`";

    /// <summary>
    /// Returns the runtime name of the string with all backticks escaped (but resulting string is not wrapped in backticks itself)
    /// </summary>
    /// <param name="s"></param>
    /// <returns></returns>
    private string GetRuntimeNameWithDoubledBackticks(string s) => GetRuntimeName(s)?.Replace("`","``");

    protected override string UnescapeWrappedNameBody(string name) => name.Replace("``","`");

    public override string EnsureFullyQualified(string databaseName, string schema, string tableName)
    {
        //if there is no schema address it as db..table (which is the same as db.dbo.table in Microsoft SQL Server)
        if (!string.IsNullOrWhiteSpace(schema))
            throw new NotSupportedException("Schema (e.g. .dbo. not supported by MySql)");

        return $"{EnsureWrapped(databaseName)}{DatabaseTableSeparator}{EnsureWrapped(tableName)}";
    }

    public override TopXResponse HowDoWeAchieveTopX(int x) => new($"LIMIT {x}",QueryComponent.Postfix);

    public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType)
    {
        //MySql doesn't require parameter declaration you just start using it like javascript
        return $"/* {proposedNewParameterName} */";
    }

    public override string Escape(string sql)
    {
        // https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
        var r = new StringBuilder(sql.Length);
        foreach (var c in sql)
        {
            r.Append(c switch
            {
                '\0'    => "\\0",
                '\'' => "\\'",
                '"' => "\"",
                '\b'    => "\\b",
                '\n'    => "\\n",
                '\r'    => "\\r",
                '\t'    => "\\t",
                '\u001a'    => "\\Z",
                '\\' => "\\",
// Pattern matching only:
// '%' => "\\%",
// '_' => "\\_",
                _   => $"{c}"
            });
        }
        return r.ToString();
    }

    public override string GetScalarFunctionSql(MandatoryScalarFunctions function) =>
        function switch
        {
            MandatoryScalarFunctions.GetTodaysDate => //this works at least as of 5.7.19
                "now()",
            MandatoryScalarFunctions.GetGuid => //using this as defaults in columns requires MySql 8 (2018)
                "(uuid())",
            MandatoryScalarFunctions.Len => "LENGTH",
            _ => throw new ArgumentOutOfRangeException(nameof(function))
        };

    public override string GetAutoIncrementKeywordIfAny() => "AUTO_INCREMENT";

    public override Dictionary<string, string> GetSQLFunctionsDictionary()
    {
        return Functions;
    }

    private static readonly Dictionary<string, string> Functions = new()
    {
        {"left", "LEFT ( string , length)"},
        {"right", "RIGHT ( string , length )"},
        {"upper", "UPPER ( string )"},
        {"substring", "SUBSTR ( str ,start , length ) "},
        {"dateadd", "DATE_ADD (date, INTERVAL value unit)"},
        {"datediff", "DATEDIFF ( date1 , date2)  "},
        {"getdate", "now()"},
        {"now", "now()"},
        {"cast", "CAST ( value AS type )"},
        {"convert", "CONVERT ( value, type ) "},
        {"case", "CASE WHEN x=y THEN 'something' WHEN x=z THEN 'something2' ELSE 'something3' END"}
    };

    public override string HowDoWeAchieveMd5(string selectSql) => $"md5({selectSql})";
}