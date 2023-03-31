using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.PostgreSql.Aggregation;

public class PostgreSqlAggregateHelper : AggregateHelper
{
    protected override IQuerySyntaxHelper GetQuerySyntaxHelper()
    {
        return new PostgreSqlSyntaxHelper();
    }

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        string interval = query.Axis.AxisIncrement switch
        {
            AxisIncrement.Day => "1 day",
            AxisIncrement.Month => "1 month",
            AxisIncrement.Year => "1 year",
            AxisIncrement.Quarter => "3 months",
            _ => throw new ArgumentOutOfRangeException(nameof(query),$"Invalid AxisIncrement {query.Axis.AxisIncrement}")
        };

        var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query,axisColumnAlias);

        var sql =
            string.Format(@"
{0}
SELECT
   {1} AS ""joinDt"",dataset.{6}
FROM
generate_series({3},
             {4},
            interval '{5}')
LEFT JOIN
(
    {2}
) dataset
ON dataset.{7} = {1}
ORDER BY 
{1}
", 
                //Anything before the SELECT
                string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert < QueryComponent.SELECT)),
                GetDatePartOfColumn(query.Axis.AxisIncrement, "generate_series.date"),
                //the entire query
                string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)), query.Axis.StartDate,
                query.Axis.EndDate,
                interval,
                countAlias,
                axisColumnAlias);

        return sql;
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        throw new NotImplementedException();
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        throw new NotImplementedException();
    }

    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql) =>
        increment switch
        {
            AxisIncrement.Day => $"{columnSql}::date",
            AxisIncrement.Month => $"to_char({columnSql},'YYYY-MM')",
            AxisIncrement.Year => $"date_part('year', {columnSql})",
            AxisIncrement.Quarter => $"to_char({columnSql},'YYYY\"Q\"Q')",
            _ => throw new ArgumentOutOfRangeException(nameof(increment), increment, null)
        };
}