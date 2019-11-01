using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.PostgreSql.Aggregation
{
    public class PostgreSqlAggregateHelper : AggregateHelper
    {
        protected override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new PostgreSqlSyntaxHelper();
        }

        protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
        {
            var syntaxHelper = new PostgreSqlSyntaxHelper();

            string interval;

            switch (query.Axis.AxisIncrement)
            {
                case AxisIncrement.Day:
                    interval = "1 day";
                    break;
                case AxisIncrement.Month:
                    interval = "1 month";
                    break;
                case AxisIncrement.Year:
                    interval = "1 year";
                    break;
                case AxisIncrement.Quarter:
                    interval = "3 months";
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            string countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);
            string axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

            WrapAxisColumnWithDatePartFunction(query,axisColumnAlias);

            string sql =
                string.Format(@"
{0}
SELECT
   {1} AS ""joinDt"",dataset.{6}
FROM
generate_series(date {3},
            date {4},
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
                    string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert >= QueryComponent.SELECT && c.LocationToInsert <= QueryComponent.Having)), query.Axis.StartDate,
                    query.Axis.EndDate,
                    interval,
                    countAlias,
                    axisColumnAlias);

            return sql;
        }

        protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
        {
            throw new System.NotImplementedException();
        }

        protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
        {
            throw new System.NotImplementedException();
        }

        public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
        {
            switch (increment)
            {
                case AxisIncrement.Day:
                    return columnSql + "::date";
                case AxisIncrement.Month:
                    return $"to_char({columnSql},'YYYY-MM')";
                case AxisIncrement.Year:
                    return $"date_part('year', {columnSql})";
                case AxisIncrement.Quarter:
                    return $"to_char({columnSql},'YYYY-\"Q\"Q')";
                default:
                    throw new ArgumentOutOfRangeException(nameof(increment), increment, null);
            }
        }
    }
}