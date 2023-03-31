using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.Oracle.Aggregation;

public class OracleAggregateHelper : AggregateHelper
{
    protected override IQuerySyntaxHelper GetQuerySyntaxHelper()
    {
        return new OracleQuerySyntaxHelper();
    }

    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
    {
        switch (increment)
        {
            case AxisIncrement.Day:
                return columnSql;
            case AxisIncrement.Month:
                return $"to_char({columnSql},'YYYY-MM')";
            case AxisIncrement.Year:
                return $"to_number(to_char({columnSql},'YYYY'))";
            case AxisIncrement.Quarter:
                return $"to_char({columnSql},'YYYY') || 'Q' || to_char({columnSql},'Q')";
            default:
                throw new ArgumentOutOfRangeException("increment");
        }
    }

    private string GetDateAxisTableDeclaration(IQueryAxis axis)
    {
        //https://stackoverflow.com/questions/8374959/how-to-populate-calendar-table-in-oracle

        //expect the date to be either '2010-01-01' or a function that evaluates to a date e.g. CURRENT_TIMESTAMP
        string startDateSql;

        //is it a date in some format or other?
        if(DateTime.TryParse(axis.StartDate.Trim('\'','"'),out DateTime start))
            startDateSql = $"to_date('{start.ToString("yyyyMMdd")}','yyyymmdd')";
        else
            startDateSql = $"to_date(to_char({axis.StartDate}, 'YYYYMMDD'), 'yyyymmdd')";//assume its some Oracle specific syntax that results in a date

        string endDateSql;
        if (DateTime.TryParse(axis.EndDate.Trim('\'', '"'), out DateTime end))
            endDateSql = $"to_date('{end.ToString("yyyyMMdd")}','yyyymmdd')";
        else
            endDateSql = $"to_date(to_char({axis.EndDate}, 'YYYYMMDD'), 'yyyymmdd')";//assume its some Oracle specific syntax that results in a date e.g. CURRENT_TIMESTAMP

        switch (axis.AxisIncrement)
        {
                
            case AxisIncrement.Year:
                return
                    string.Format(
                        @"
with calendar as (
        select add_months({0},12* (rownum - 1)) as dt
        from dual
        connect by rownum <= 1+
floor(months_between({1}, {0}) /12)
    )",
                        startDateSql,
                        endDateSql);

            case AxisIncrement.Day:
                return
                    string.Format(
                        @"
with calendar as (
        select {0} + (rownum - 1) as dt
        from dual
        connect by rownum <= 1+
floor({1} - {0})
    )",
                        startDateSql,
                        endDateSql);
            case AxisIncrement.Month:
                return 
                    string.Format(
                        @"
with calendar as (
        select add_months({0},rownum - 1) as dt
        from dual
        connect by rownum <= 1+
floor(months_between({1}, {0}))
    )",
                        startDateSql,
                        endDateSql);
            case AxisIncrement.Quarter:

                return
                    string.Format(
                        @"
with calendar as (
        select add_months({0},3* (rownum - 1)) as dt
        from dual
        connect by rownum <= 1+
floor(months_between({1}, {0}) /3)
    )",
                        startDateSql,
                        endDateSql);
            default:
                throw new NotImplementedException();
        }         
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

        string calendar = GetDateAxisTableDeclaration(query.Axis);

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
            string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert >= QueryComponent.SELECT && c.LocationToInsert <= QueryComponent.Having)),
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