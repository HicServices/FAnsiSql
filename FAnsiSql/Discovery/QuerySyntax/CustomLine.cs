using System;

namespace FAnsi.Discovery.QuerySyntax;

/// <summary>
/// An arbitrary string to be injected into an SQL query being built by an ISqlQueryBuilder.  This is needed to handle differences in Database Query Engine Implementations
/// e.g. Top X is done as part of SELECT in Microsoft Sql Server (e.g. select top x * from bob) while in MySql it is done as part of Postfix (e.g. select * from bob LIMIT 1)
/// (See IQuerySyntaxHelper.HowDoWeAchieveTopX).
/// 
/// <para>Each CustomLine must have an QueryComponent of the Query that it relates to (LocationToInsert) and may have a CustomLineRole. </para>
/// 
/// <para>AggregateBuilder relies heavily on CustomLine because of the complexity of cross database platform GROUP BY (e.g. dynamic pivot with calendar table).  Basically converting
/// the entire query into CustomLines and passing off implementation to the specific database engine (See IAggregateHelper.BuildAggregate).</para>
/// </summary>
public sealed class CustomLine(string text, QueryComponent locationToInsert)
{
    public string Text { get; set; } = string.IsNullOrWhiteSpace(text) ? text : text.Trim();
    public QueryComponent LocationToInsert { get; set; } = locationToInsert;

    public CustomLineRole Role { get; set; }

    /// <summary>
    /// The line of code that caused the CustomLine to be created, this can be a StackTrace passed into the constructor or calculated automatically by CustomLine
    /// </summary>
    public string StackTrace { get; private set; } = Environment.StackTrace;

    public override string ToString() => Text;

    /// <summary>
    /// Returns the section of <see cref="Text"/> which does not include any alias e.g. returns "UPPER('a')" from "UPPER('a') as a"
    /// </summary>
    /// <param name="syntaxHelper"></param>
    /// <returns></returns>
    public string GetTextWithoutAlias(IQuerySyntaxHelper syntaxHelper)
    {
        syntaxHelper.SplitLineIntoSelectSQLAndAlias(Text, out var withoutAlias, out _);
        return withoutAlias;
    }

    /// <summary>
    /// Returns the alias section of <see cref="Text"/> e.g. returns "a" from "UPPER('a') as a"
    /// </summary>
    /// <param name="syntaxHelper"></param>
    /// <returns></returns>
    public string? GetAliasFromText(IQuerySyntaxHelper syntaxHelper)
    {
        syntaxHelper.SplitLineIntoSelectSQLAndAlias(Text, out _, out var alias);
        return string.IsNullOrWhiteSpace(alias) ? null : alias;
    }
}