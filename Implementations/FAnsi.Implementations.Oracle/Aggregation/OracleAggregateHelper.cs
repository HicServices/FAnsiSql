using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.Oracle.Aggregation
{
    public class OracleAggregateHelper : AggregateHelper
    {
        public override string BuildAggregate(List<CustomLine> queryLines, IQueryAxis axisIfAny, bool pivot)
        {
            if (!pivot && axisIfAny == null)
                return string.Join(Environment.NewLine, queryLines);


            //axis only
            if (!pivot)
                return BuildAxisOnlyAggregate(queryLines, axisIfAny);

            throw new System.NotImplementedException();
        }
        public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
        {
            switch (increment)
            {
                case AxisIncrement.Day:
                    return $"to_char({columnSql},'YYYY-MM-dd')";
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
                case AxisIncrement.Month:
                case AxisIncrement.Quarter:
                default:
                    throw new NotImplementedException();
            }         
        }

        private string BuildAxisOnlyAggregate(List<CustomLine> lines, IQueryAxis axis)
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

            var syntaxHelper = new OracleQuerySyntaxHelper();

            GetAggregateAxisBits(syntaxHelper,lines,
                out CustomLine countSelectLine,
                out string countSqlWithoutAlias,
                out string countAlias,
                out CustomLine axisColumn,
                out string axisColumnWithoutAlias,
                out string axisColumnAlias);

            WrapAxisColumnWithDatePartFunction(axisColumn, lines, axis, axisColumnWithoutAlias, axisColumnAlias);

            string calendar = GetDateAxisTableDeclaration(axis);

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
                string.Join(Environment.NewLine, lines.Where(c => c.LocationToInsert < QueryComponent.SELECT)),
                //then add the calendar
                calendar,
                GetDatePartOfColumn(axis.AxisIncrement, "dt"),
                countAlias,
                //the entire query
                string.Join(Environment.NewLine, lines.Where(c => c.LocationToInsert >= QueryComponent.SELECT && c.LocationToInsert <= QueryComponent.Having)),
                axisColumnAlias

                );

        }

    }
}