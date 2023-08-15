using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.Oracle.Aggregation;

public sealed class OracleAggregateHelper : AggregateHelper
{
    public static readonly OracleAggregateHelper Instance=new();
    private OracleAggregateHelper() {}
    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => OracleQuerySyntaxHelper.Instance;

    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql) =>
        increment switch
        {
            AxisIncrement.Day => columnSql,
            AxisIncrement.Month => $"to_char({columnSql},'YYYY-MM')",
            AxisIncrement.Year => $"to_number(to_char({columnSql},'YYYY'))",
            AxisIncrement.Quarter => $"to_char({columnSql},'YYYY') || 'Q' || to_char({columnSql},'Q')",
            _ => throw new ArgumentOutOfRangeException(nameof(increment))
        };

    private string GetDateAxisTableDeclaration(IQueryAxis axis)
    {
        //https://stackoverflow.com/questions/8374959/how-to-populate-calendar-table-in-oracle

        //expect the date to be either '2010-01-01' or a function that evaluates to a date e.g. CURRENT_TIMESTAMP

        var startDateSql =
            //is it a date in some format or other?
            DateTime.TryParse(axis.StartDate.Trim('\'', '"'), out var start)
                ? $"to_date('{start:yyyyMMdd}','yyyymmdd')"
                : $"to_date(to_char({axis.StartDate}, 'YYYYMMDD'), 'yyyymmdd')"; //assume its some Oracle specific syntax that results in a date

        var endDateSql = DateTime.TryParse(axis.EndDate.Trim('\'', '"'), out var end)
            ? $"to_date('{end:yyyyMMdd}','yyyymmdd')"
            : $"to_date(to_char({axis.EndDate}, 'YYYYMMDD'), 'yyyymmdd')"; //assume its some Oracle specific syntax that results in a date e.g. CURRENT_TIMESTAMP

        return axis.AxisIncrement switch
        {
            AxisIncrement.Year => $@"
with calendar as (
        select add_months({startDateSql},12* (rownum - 1)) as dt
        from dual
        connect by rownum <= 1+
floor(months_between({endDateSql}, {startDateSql}) /12)
    )",
            AxisIncrement.Day => $@"
with calendar as (
        select {startDateSql} + (rownum - 1) as dt
        from dual
        connect by rownum <= 1+
floor({endDateSql} - {startDateSql})
    )",
            AxisIncrement.Month => $@"
with calendar as (
        select add_months({startDateSql},rownum - 1) as dt
        from dual
        connect by rownum <= 1+
floor(months_between({endDateSql}, {startDateSql}))
    )",
            AxisIncrement.Quarter => $@"
with calendar as (
        select add_months({startDateSql},3* (rownum - 1)) as dt
        from dual
        connect by rownum <= 1+
floor(months_between({endDateSql}, {startDateSql}) /3)
    )",
            _ => throw new NotImplementedException()
        };
    }

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        //we are trying to produce something like this:
        /*
with calendar as (
    select add_months(to_date('20010101','yyyymmdd'),12* (rownum - 1)) as dt
    from dual
    connect by rownum <= 1+
floor(months_between(to_date(to_char(CURRENT_TIMESTAMP, 'YYYYMMDD'), 'yyyymmdd'), to_date('20010101','yyyymmdd')) /12)
)
select
to_char(dt ,'YYYY') dt,
count(*) NumRecords
from calendar
join 
"TEST"."HOSPITALADMISSIONS" on 
to_char(dt ,'YYYY') = to_char("TEST"."HOSPITALADMISSIONS"."ADMISSION_DATE" ,'YYYY') 
group by 
dt
order by dt*/

        var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var calendar = GetDateAxisTableDeclaration(query.Axis);

        return string.Format(
            @"
{0}
{1}
SELECT 
{2} AS ""joinDt"",dataset.{3}
FROM
calendar
LEFT JOIN
(
    {4}
) dataset
ON dataset.{5} = {2}
ORDER BY 
{2}
",
            //add everything pre SELECT
            string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert < QueryComponent.SELECT)),
            //then add the calendar
            calendar,
            GetDatePartOfColumn(query.Axis.AxisIncrement, "dt"),
            countAlias,
            //the entire query
            string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            axisColumnAlias

        );

    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        throw new NotImplementedException();
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        throw new NotImplementedException();
    }
}