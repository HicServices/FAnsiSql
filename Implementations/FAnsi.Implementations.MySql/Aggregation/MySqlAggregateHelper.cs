using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.MySql.Aggregation
{
    public class MySqlAggregateHelper : IAggregateHelper
    {
        private string GetDateAxisTableDeclaration(IQueryAxis axis)
        {
            
            //QueryComponent.JoinInfoJoin
            return 
                string.Format(
            @"

    SET @startDate = {0};
    SET @endDate = {1};

    drop temporary table if exists dateAxis;

    create temporary table dateAxis
    (
	    dt DATE
    );

insert into dateAxis

    SELECT distinct (@startDate + INTERVAL c.number {2}) AS date
FROM (SELECT singles + tens + hundreds number FROM 
( SELECT 0 singles
UNION ALL SELECT   1 UNION ALL SELECT   2 UNION ALL SELECT   3
UNION ALL SELECT   4 UNION ALL SELECT   5 UNION ALL SELECT   6
UNION ALL SELECT   7 UNION ALL SELECT   8 UNION ALL SELECT   9
) singles JOIN 
(SELECT 0 tens
UNION ALL SELECT  10 UNION ALL SELECT  20 UNION ALL SELECT  30
UNION ALL SELECT  40 UNION ALL SELECT  50 UNION ALL SELECT  60
UNION ALL SELECT  70 UNION ALL SELECT  80 UNION ALL SELECT  90
) tens  JOIN 
(SELECT 0 hundreds
UNION ALL SELECT  100 UNION ALL SELECT  200 UNION ALL SELECT  300
UNION ALL SELECT  400 UNION ALL SELECT  500 UNION ALL SELECT  600
UNION ALL SELECT  700 UNION ALL SELECT  800 UNION ALL SELECT  900
) hundreds 
ORDER BY number DESC) c  
WHERE c.number BETWEEN 0 and 1000;

delete from dateAxis where dt > @endDate;",
            axis.StartDate,
            axis.EndDate,
            axis.AxisIncrement);
        }

        public string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
        {
            switch (increment)
            {
                case AxisIncrement.Day:
                    return "DATE(" + columnSql + ")";
                case AxisIncrement.Month:
                    return "DATE_FORMAT("+columnSql+",'%Y-%m')";
                case AxisIncrement.Year:
                    return "YEAR(" + columnSql + ")";
                case AxisIncrement.Quarter:
                    return "CONCAT(YEAR(" + columnSql + "),'Q',QUARTER(" + columnSql + "))";
                default:
                    throw new ArgumentOutOfRangeException("increment");
            }

        }

        

        private string BuildAxisOnlyAggregate(List<CustomLine> lines, IQueryAxis axis)
        {
            var syntaxHelper = new MySqlQuerySyntaxHelper();
            
            var countSelectLine = lines.Single(l => l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.CountFunction);

            string countSqlWithoutAlias;
            string countAlias;
            syntaxHelper.SplitLineIntoSelectSQLAndAlias(countSelectLine.Text, out countSqlWithoutAlias, out countAlias);

            //Deal with the axis dimension which is currently `mydb`.`mytbl`.`mycol` and needs to become YEAR(`mydb`.`mytbl`.`mycol`) As joinDt 
            var axisColumn = lines.Single(l => l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.Axis);

            string axisColumnWithoutAlias;
            string axisColumnAlias;
            syntaxHelper.SplitLineIntoSelectSQLAndAlias(axisColumn.Text, out axisColumnWithoutAlias, out axisColumnAlias);

            var axisGroupBy = lines.Single(l => l.LocationToInsert == QueryComponent.GroupBy && l.Role == CustomLineRole.Axis);

            if (string.IsNullOrWhiteSpace(axisColumnAlias))
                axisColumnAlias = "joinDt";

            var axisColumnEndedWithComma = axisColumn.Text.EndsWith(",");
            axisColumn.Text = GetDatePartOfColumn(axis.AxisIncrement, axisColumnWithoutAlias) + " AS " + axisColumnAlias + (axisColumnEndedWithComma?",":"");

            var groupByEndedWithComma = axisGroupBy.Text.EndsWith(",");
            axisGroupBy.Text = GetDatePartOfColumn(axis.AxisIncrement, axisColumnWithoutAlias) + (groupByEndedWithComma ? "," : "");
            

            return string.Format(
                @"
{0}
{1}

SELECT 
{2} AS joinDt,dataset.{3}
FROM
dateAxis
LEFT JOIN
(
    {4}
) dataset
ON dataset.{5} = {2}
ORDER BY 
{2}
"
                ,
                string.Join(Environment.NewLine, lines.Where(c => c.LocationToInsert < QueryComponent.SELECT)),
                GetDateAxisTableDeclaration(axis),

                GetDatePartOfColumn(axis.AxisIncrement,"dateAxis.dt"),
                countAlias,
                
                //the entire query
                string.Join(Environment.NewLine, lines.Where(c => c.LocationToInsert >= QueryComponent.SELECT && c.LocationToInsert <= QueryComponent.Having)),
                axisColumnAlias
                ).Trim();

        }

        private string BuildPivotAndAxisAggregate(List<CustomLine> lines, IQueryAxis axis)
        {
            string axisColumnWithoutAlias;
            MySqlQuerySyntaxHelper syntaxHelper;
            string part1 = GetPivotPart1(lines, out syntaxHelper,out axisColumnWithoutAlias);
            
            return string.Format(@"
{0}

{1}

{2}

SET @sql =

CONCAT(
'
SELECT 
{3} as joinDt,',@columnsSelectFromDataset,'
FROM
dateAxis
LEFT JOIN
(
    {4}
    {5} AS joinDt,
'
    ,@columnsSelectCases,
'
{6}
group by
{5}
) dataset
ON {3} = dataset.joinDt
ORDER BY 
{3}
');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;",
                string.Join(Environment.NewLine, lines.Where(l => l.LocationToInsert < QueryComponent.SELECT)),
                GetDateAxisTableDeclaration(axis),
                part1,
                syntaxHelper.Escape(GetDatePartOfColumn(axis.AxisIncrement, "dateAxis.dt")),
                string.Join(Environment.NewLine, lines.Where(c => c.LocationToInsert == QueryComponent.SELECT)),

                //the from including all table joins and where but no calendar table join
                syntaxHelper.Escape(GetDatePartOfColumn(axis.AxisIncrement,axisColumnWithoutAlias)),

                //the order by (should be count so that heavy populated columns come first)
                syntaxHelper.Escape(string.Join(Environment.NewLine, lines.Where(c => c.LocationToInsert >= QueryComponent.FROM && c.LocationToInsert <= QueryComponent.WHERE)))
                );
        }
        
        private string BuildPivotOnlyAggregate(List<CustomLine> lines)
        {
            MySqlQuerySyntaxHelper syntaxHelper;
            string axisColumnWithoutAlias;
            string part1 = GetPivotPart1(lines, out syntaxHelper, out axisColumnWithoutAlias);

            
            var nonPivotColumn = lines.Where(l => l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.None).ToArray();
            if(nonPivotColumn.Length != 1)
                throw new Exception("Pivot is only valid when there are 3 SELECT columns, an aggregate (e.g. count(*)), a pivot and a final column");
                        
            return string.Format(@"
{0}

{1}

SET @sql =

CONCAT(
'
SELECT 
{2}',@columnsSelectCases,'

{3}');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;",
                string.Join(Environment.NewLine, lines.Where(l => l.LocationToInsert < QueryComponent.SELECT)),
                part1,
                nonPivotColumn[0],
                syntaxHelper.Escape(string.Join(Environment.NewLine, lines.Where(c => c.LocationToInsert >= QueryComponent.FROM)))
            );
        }

        /// <summary>
        /// Returns the section of the PIVOT which identifies unique values.  For MySql this is done by assembling a massive CASE statement.
        /// </summary>
        /// <param name="lines"></param>
        /// <param name="syntaxHelper"></param>
        /// <returns></returns>
        private static string GetPivotPart1(List<CustomLine> lines, out MySqlQuerySyntaxHelper syntaxHelper, out string axisColumnWithoutAlias)
        {
            syntaxHelper = new MySqlQuerySyntaxHelper();

            var pivotSelectLine = lines.Single(l =>
                l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.Pivot);

            string pivotSqlWithoutAlias;
            string pivotAlias;
            syntaxHelper.SplitLineIntoSelectSQLAndAlias(pivotSelectLine.Text, out pivotSqlWithoutAlias, out pivotAlias);

            var countSelectLine = lines.Single(l =>
                l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.CountFunction);

            string countSqlWithoutAlias;
            string countAlias;
            syntaxHelper.SplitLineIntoSelectSQLAndAlias(countSelectLine.Text, out countSqlWithoutAlias, out countAlias);

            string aggregateMethod;
            string aggregateParameter;
            syntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out aggregateMethod,
                out aggregateParameter);

            if (aggregateParameter.Equals("*"))
                aggregateParameter = "1";

            var joinDtColumn = lines.SingleOrDefault(l =>
                l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.Axis);
            
            //if there is an axis we must ensure we only pull pivot values where the values appear in that axis range
            string whereDateColumnNotNull = "";
            
            if(joinDtColumn != null)
            {
                string axisColumnAlias;
                syntaxHelper.SplitLineIntoSelectSQLAndAlias(joinDtColumn.Text, out axisColumnWithoutAlias, out axisColumnAlias);
                
                whereDateColumnNotNull += lines.Any(l => l.LocationToInsert == QueryComponent.WHERE) ? "AND " : "WHERE ";
                whereDateColumnNotNull += axisColumnWithoutAlias + " IS NOT NULL";
            }
            else
                axisColumnWithoutAlias = null;

            //work out how to order the pivot columns
            string orderBy = countSqlWithoutAlias + " desc"; //default, order by the count(*) / sum(*) etc column desc

            //theres an explicit topX so order by it verbatim instead
            var topXOrderByLine =
                lines.SingleOrDefault(c => c.LocationToInsert == QueryComponent.OrderBy && c.Role == CustomLineRole.TopX);
            if (topXOrderByLine != null)
                orderBy = topXOrderByLine.Text;

            //if theres a topX limit postfix line (See MySqlQuerySyntaxHelper.HowDoWeAchieveTopX) add that too
            var topXLimitLine =
                lines.SingleOrDefault(c => c.LocationToInsert == QueryComponent.Postfix && c.Role == CustomLineRole.TopX);
            string topXLimitSqlIfAny = topXLimitLine != null ? topXLimitLine.Text : "";

            string havingSqlIfAny = string.Join(Environment.NewLine,
                lines.Where(l => l.LocationToInsert == QueryComponent.Having).Select(l => l.Text));

            return string.Format(@"
SET SESSION group_concat_max_len = 1000000; 

DROP TEMPORARY TABLE IF EXISTS pivotValues;

/*Get the unique values in the pivot column into a temporary table ordered by size of the count*/
CREATE TEMPORARY TABLE pivotValues AS (
SELECT
{1} as piv
{3}
{4}
group by
{1}
{7}
order by
{6}
{5}
);

/* Build case when x='fish' then 1 end as 'fish', case when x='cammel' then 1 end as 'cammel' etc*/
SET @columnsSelectCases = NULL;
SELECT
  GROUP_CONCAT(
    CONCAT(
      '{0}(case when {1} = ''', REPLACE(pivotValues.piv,'\'','\\\''), ''' then {2} end) AS `', pivotValues.piv,'`'
    )
  ) INTO @columnsSelectCases
FROM
pivotValues;

/* Build dataset.fish, dataset.cammel etc*/
SET @columnsSelectFromDataset = NULL;
SELECT
  GROUP_CONCAT(
    CONCAT(
      'dataset.`', pivotValues.piv,'`')
  ) INTO @columnsSelectFromDataset
FROM
pivotValues;
",
                aggregateMethod,
                pivotSqlWithoutAlias,
                aggregateParameter,

                //the from including all table joins and where but no calendar table join
                string.Join(Environment.NewLine,
                    lines.Where(l =>
                        l.LocationToInsert >= QueryComponent.FROM && l.LocationToInsert <= QueryComponent.WHERE &&
                        l.Role != CustomLineRole.Axis)),
                whereDateColumnNotNull,
                topXLimitSqlIfAny,
                orderBy,
                havingSqlIfAny
            );
            
        }


        
        public string BuildAggregate(List<CustomLine> queryLines, IQueryAxis axisIfAny, bool pivot)
        {
            if (!pivot && axisIfAny == null)
                return string.Join(Environment.NewLine, queryLines);

            //axis only
            if (!pivot)
                return BuildAxisOnlyAggregate(queryLines,axisIfAny);
            
            //axis and pivot (cannot pivot without axis)
            if (axisIfAny == null)
                return BuildPivotOnlyAggregate(queryLines);

            return BuildPivotAndAxisAggregate(queryLines, axisIfAny);
            
        }

        //so janky to double select GROUP_Concat just so we can get dataset* except join.dt -- can we do it once into @columns then again into the other

//use mysql;

//    SET @startDate = '1920-01-01';
//    SET @endDate = now();

//    drop temporary table if exists dateAxis;

//    create temporary table dateAxis
//    (
//        dt DATE
//    );

//insert into dateAxis

//    SELECT distinct (@startDate + INTERVAL c.number Year) AS date
//FROM (SELECT singles + tens + hundreds number FROM 
//( SELECT 0 singles
//UNION ALL SELECT   1 UNION ALL SELECT   2 UNION ALL SELECT   3
//UNION ALL SELECT   4 UNION ALL SELECT   5 UNION ALL SELECT   6
//UNION ALL SELECT   7 UNION ALL SELECT   8 UNION ALL SELECT   9
//) singles JOIN 
//(SELECT 0 tens
//UNION ALL SELECT  10 UNION ALL SELECT  20 UNION ALL SELECT  30
//UNION ALL SELECT  40 UNION ALL SELECT  50 UNION ALL SELECT  60
//UNION ALL SELECT  70 UNION ALL SELECT  80 UNION ALL SELECT  90
//) tens  JOIN 
//(SELECT 0 hundreds
//UNION ALL SELECT  100 UNION ALL SELECT  200 UNION ALL SELECT  300
//UNION ALL SELECT  400 UNION ALL SELECT  500 UNION ALL SELECT  600
//UNION ALL SELECT  700 UNION ALL SELECT  800 UNION ALL SELECT  900
//) hundreds 
//ORDER BY number DESC) c  
//WHERE c.number BETWEEN 0 and 1000;

//delete from dateAxis where dt > @endDate;

//SET SESSION group_concat_max_len = 1000000; 

//SET @columns = NULL;
//SELECT
//  GROUP_CONCAT(DISTINCT
//    CONCAT(
//      'count(case when `test`.`biochemistry`.`hb_extract` = ''',
//      b.`Pivot`,
//      ''' then 1 end) AS `',
//      b.`Pivot`,'`'
//    ) order by b.`CountName` desc
//  ) INTO @columns
//FROM
//(
//select `test`.`biochemistry`.`hb_extract` AS Pivot, count(*) AS CountName
//FROM 
//`test`.`biochemistry`
//group by `test`.`biochemistry`.`hb_extract`
//) as b;


//SET @columnNames = NULL;
//SELECT
//  GROUP_CONCAT(DISTINCT
//    CONCAT(
//      'dataset.`',b.`Pivot`,'`') order by b.`CountName` desc
//  ) INTO @columnNames
//FROM
//(
//select `test`.`biochemistry`.`hb_extract` AS Pivot, count(*) AS CountName
//FROM 
//`test`.`biochemistry`
//group by `test`.`biochemistry`.`hb_extract`
//) as b;




//SET @sql =


//CONCAT(
//'
//SELECT 
//YEAR(dateAxis.dt) AS joinDt,',@columnNames,'
//FROM
//dateAxis
//LEFT JOIN
//(
//    /*HbsByYear*/
//SELECT
//    YEAR(`test`.`biochemistry`.`sample_date`) AS joinDt,
//'
//    ,@columns,
//'
//FROM 
//`test`.`biochemistry`
//group by
//YEAR(`test`.`biochemistry`.`sample_date`)
//) dataset
//ON dataset.joinDt = YEAR(dateAxis.dt)
//ORDER BY 
//YEAR(dateAxis.dt)
//');

//PREPARE stmt FROM @sql;
//EXECUTE stmt;
//DEALLOCATE PREPARE stmt;

    }
}
