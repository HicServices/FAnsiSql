using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.MySql.Aggregation
{
    public class MySqlAggregateHelper : AggregateHelper
    {
        private string GetDateAxisTableDeclaration(IQueryAxis axis)
        {
            //if the axis is days then there are likely to be thousands of them but if we start adding thousands of years
            //mysql date falls over with overflow exceptions
            string thousands =
                axis.AxisIncrement == AxisIncrement.Day ? 
                @"JOIN 
(SELECT 0 thousands
UNION ALL SELECT  1000 UNION ALL SELECT  2000 UNION ALL SELECT  3000
UNION ALL SELECT  4000 UNION ALL SELECT  5000 UNION ALL SELECT  6000
UNION ALL SELECT  7000 UNION ALL SELECT  8000 UNION ALL SELECT  9000
) thousands" : "";

            string plusThousands = axis.AxisIncrement == AxisIncrement.Day ? "+ thousands":"";

            //QueryComponent.JoinInfoJoin
            return 
            $@"

    SET @startDate = {axis.StartDate};
    SET @endDate = {axis.EndDate};

    drop temporary table if exists dateAxis;

    create temporary table dateAxis
    (
	    dt DATE
    );

insert into dateAxis

    SELECT distinct (@startDate + INTERVAL c.number {axis.AxisIncrement}) AS date
FROM (SELECT singles + tens + hundreds {plusThousands} number FROM 
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
{thousands}
ORDER BY number DESC) c  
WHERE c.number BETWEEN 0 and 10000;

delete from dateAxis where dt > @endDate;";
        }

        public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
        {
            return increment switch
            {
                AxisIncrement.Day => $"DATE({columnSql})",
                AxisIncrement.Month => $"DATE_FORMAT({columnSql},'%Y-%m')",
                AxisIncrement.Year => $"YEAR({columnSql})",
                AxisIncrement.Quarter => $"CONCAT(YEAR({columnSql}),'Q',QUARTER({columnSql}))",
                _ => throw new ArgumentOutOfRangeException(nameof(increment))
            };
        }


        protected override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new MySqlQuerySyntaxHelper();
        }

        protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
        {
            var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);
            var axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";
            
            WrapAxisColumnWithDatePartFunction(query,axisColumnAlias);
            

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
                string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert < QueryComponent.SELECT)),
                GetDateAxisTableDeclaration(query.Axis),

                GetDatePartOfColumn(query.Axis.AxisIncrement,"dateAxis.dt"),
                countAlias,
                
                //the entire query
                string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert >= QueryComponent.SELECT && c.LocationToInsert <= QueryComponent.Having)),
                axisColumnAlias
                ).Trim();

        }
        
        protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
        {
            string axisColumnWithoutAlias = query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper);
            string part1 = GetPivotPart1(query);
            
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
                string.Join(Environment.NewLine, query.Lines.Where(l => l.LocationToInsert < QueryComponent.SELECT)),
                GetDateAxisTableDeclaration(query.Axis),
                part1,
                query.SyntaxHelper.Escape(GetDatePartOfColumn(query.Axis.AxisIncrement, "dateAxis.dt")),
                string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert == QueryComponent.SELECT)),

                //the from including all table joins and where but no calendar table join
                query.SyntaxHelper.Escape(GetDatePartOfColumn(query.Axis.AxisIncrement,axisColumnWithoutAlias)),

                //the order by (should be count so that heavy populated columns come first)
                string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert >= QueryComponent.FROM && c.LocationToInsert <= QueryComponent.WHERE).Select(x=> query.SyntaxHelper.Escape(x.Text)))
                );
        }
        
        protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
        {
            string part1 = GetPivotPart1(query);
            
            string joinAlias = nonPivotColumn.GetAliasFromText(query.SyntaxHelper);

            return string.Format(@"
{0}

{1}

SET @sql =

CONCAT(
'
SELECT 
{2}',@columnsSelectCases,'

{3}
GROUP BY 
{4}
ORDER BY 
{4}
{5}
');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;",
                string.Join(Environment.NewLine, query.Lines.Where(l => l.LocationToInsert < QueryComponent.SELECT)),
                part1,
                nonPivotColumn,

                //everything inclusive of FROM but stopping before GROUP BY 
                query.SyntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert >= QueryComponent.FROM && c.LocationToInsert < QueryComponent.GroupBy))),
                
                joinAlias,

                //any HAVING SQL
                query.SyntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert == QueryComponent.Having)))
            );
        }

        /// <summary>
        /// Returns the section of the PIVOT which identifies unique values.  For MySql this is done by assembling a massive CASE statement.
        /// </summary>
        /// <param name="lines"></param>
        /// <param name="syntaxHelper"></param>
        /// <returns></returns>
        private static string GetPivotPart1(AggregateCustomLineCollection query)
        {
            string pivotSqlWithoutAlias = query.PivotSelect.GetTextWithoutAlias(query.SyntaxHelper);

            string countSqlWithoutAlias = query.CountSelect.GetTextWithoutAlias(query.SyntaxHelper);

            string aggregateMethod;
            string aggregateParameter;
            query.SyntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out aggregateMethod,
                out aggregateParameter);

            if (aggregateParameter.Equals("*"))
                aggregateParameter = "1";

            
            //if there is an axis we must ensure we only pull pivot values where the values appear in that axis range
            string whereDateColumnNotNull = "";
            
            if(query.AxisSelect != null)
            {
                var axisColumnWithoutAlias = query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper);
                
                whereDateColumnNotNull += query.Lines.Any(l => l.LocationToInsert == QueryComponent.WHERE) ? "AND " : "WHERE ";
                whereDateColumnNotNull += $"{axisColumnWithoutAlias} IS NOT NULL";
            }

            //work out how to order the pivot columns
            string orderBy = $"{countSqlWithoutAlias} desc"; //default, order by the count(*) / sum(*) etc column desc

            //theres an explicit topX so order by it verbatim instead
            var topXOrderByLine =
                query.Lines.SingleOrDefault(c => c.LocationToInsert == QueryComponent.OrderBy && c.Role == CustomLineRole.TopX);
            if (topXOrderByLine != null)
                orderBy = topXOrderByLine.Text;

            //if theres a topX limit postfix line (See MySqlQuerySyntaxHelper.HowDoWeAchieveTopX) add that too
            var topXLimitLine =
                query.Lines.SingleOrDefault(c => c.LocationToInsert == QueryComponent.Postfix && c.Role == CustomLineRole.TopX);
            string topXLimitSqlIfAny = topXLimitLine != null ? topXLimitLine.Text : "";

            string havingSqlIfAny = string.Join(Environment.NewLine,
                query.Lines.Where(l => l.LocationToInsert == QueryComponent.Having).Select(l => l.Text));

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

/* Build case when x='fish' then 1 else null end as 'fish', case when x='cammel' then 1 end as 'cammel' etc*/
SET @columnsSelectCases = NULL;
SELECT
  GROUP_CONCAT(
    CONCAT(
      '{0}(case when {1} = \'', REPLACE(pivotValues.piv,'\'','\\\''), '\' then {2} else null end) AS `', pivotValues.piv,'`'
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
                    query.Lines.Where(l =>
                        l.LocationToInsert >= QueryComponent.FROM && l.LocationToInsert <= QueryComponent.WHERE &&
                        l.Role != CustomLineRole.Axis)),
                whereDateColumnNotNull,
                topXLimitSqlIfAny,
                orderBy,
                havingSqlIfAny
            );
            
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
//      'count(case when `test`.`biochemistry`.`hb_extract` = \'',
//      b.`Pivot`,
//      \'' then 1 else null end) AS `',
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
