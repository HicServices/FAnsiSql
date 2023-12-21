﻿using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.MicrosoftSQL.Aggregation;

public sealed class MicrosoftSQLAggregateHelper : AggregateHelper
{
    private static string GetDateAxisTableDeclaration(IQueryAxis axis)
    {
        //if pivot dimension is set then this code appears inside dynamic SQL constant string that will be Exec'd so we have to escape single quotes
        var startDateSql = axis.StartDate;
        var endDateSql = axis.EndDate;

        return $"""
                
                    DECLARE	@startDate DATE
                    DECLARE	@endDate DATE
                
                    SET @startDate = {startDateSql}
                    SET @endDate = {endDateSql}
                
                    DECLARE @dateAxis TABLE
                    (
                	    dt DATE
                    )
                
                    DECLARE @currentDate DATE = @startDate
                
                    WHILE @currentDate <= @endDate
                    BEGIN
                	    INSERT INTO @dateAxis
                		    SELECT @currentDate
                
                	    SET @currentDate = DATEADD({axis.AxisIncrement}, 1, @currentDate)
                
                    END

                """;

    }

    /// <summary>
    /// Takes the field name/transform from the dataset and wraps it with the date adjustment function specified by the AxisIncrement
    /// </summary>
    /// <param name="increment"></param>
    /// <param name="columnSql"></param>
    /// <returns></returns>
    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql) =>
        increment switch
        {
            AxisIncrement.Day =>
                $" Convert(date, {columnSql})" //Handles when there are times in the field by always converting to date
            ,
            AxisIncrement.Month => $" CONVERT(nvarchar(7),{columnSql},126)" //returns 2015-01
            ,
            AxisIncrement.Year => $" YEAR({columnSql})" //returns 2015
            ,
            AxisIncrement.Quarter =>
                $" DATENAME(year, {columnSql}) +'Q' + DATENAME(quarter,{columnSql})" //returns 2015Q1
            ,
            _ => throw new ArgumentOutOfRangeException(nameof(increment))
        };

    /// <summary>
    /// Gives you the equivalency check for the given axis joined to column1 column.  Use this in the JOIN SQL generated by AggregateBuilder
    /// </summary>
    /// <param name="increment">Step size (day, month, year, quarter)</param>
    /// <param name="column1">The column name or transform from the dataset</param>
    /// <param name="column2">The axis column e.g. axis.dt</param>
    /// <returns></returns>
    public string GetDatePartBasedEqualsBetweenColumns(AxisIncrement increment, string column1, string column2) =>
        increment switch
        {
            AxisIncrement.Day =>
                $"{GetDatePartOfColumn(increment, column1)}={column2}" //truncate any time off column1, column2 is the axis column which never has time anyway
            ,
            AxisIncrement.Month =>
                $"YEAR({column1}) = YEAR({column2}) AND MONTH({column1}) = MONTH({column2})" //for performance
            ,
            AxisIncrement.Year =>
                $"{GetDatePartOfColumn(increment, column1)}={GetDatePartOfColumn(increment, column2)}",
            AxisIncrement.Quarter =>
                $"YEAR({column1}) = YEAR({column2}) AND DATEPART(QUARTER, {column1}) = DATEPART(QUARTER, {column2})",
            _ => throw new ArgumentOutOfRangeException(nameof(increment))
        };

    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => MicrosoftQuerySyntaxHelper.Instance;

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query,axisColumnAlias);


        return string.Format(
            """

            {0}
            {1}

            SELECT
            {2} AS joinDt,dataset.{3}
            FROM
            @dateAxis axis
            LEFT JOIN
            (
                {4}
            ) dataset
            ON dataset.{5} = {2}
            ORDER BY
            {2}

            """
            ,
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
            GetDateAxisTableDeclaration(query.Axis),

            GetDatePartOfColumn(query.Axis.AxisIncrement, "axis.dt"),
            countAlias,

            //the entire query
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            axisColumnAlias
        ).Trim();
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        var syntaxHelper = query.SyntaxHelper;

        var part1 = GetPivotPart1(query, out var pivotAlias, out var countAlias, out var axisColumnAlias);

        //The dynamic query in which we assemble a query string and EXECUTE it
        var part2 = string.Format("""

                                  /*DYNAMIC PIVOT*/
                                  declare @Query varchar(MAX)

                                  SET @Query = '
                                  {0}
                                  {1}

                                  /*Would normally be Select * but must make it IsNull to ensure we see 0s instead of null*/
                                  select '+@FinalSelectList+'
                                  from
                                  (

                                  SELECT
                                      {5} as joinDt,
                                      {4},
                                      {3}
                                      FROM
                                      @dateAxis axis
                                      LEFT JOIN
                                      (
                                          {2}
                                      )ds
                                      on {5} = ds.{6}
                                  ) s
                                  PIVOT
                                  (
                                  	sum({3})
                                  	for {4} in ('+@Columns+') --The dynamic Column list we just fetched at top of query
                                  ) piv
                                  ORDER BY
                                  joinDt'

                                  EXECUTE(@Query)

                                  """,
            syntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT))),
            syntaxHelper.Escape(GetDateAxisTableDeclaration(query.Axis)),

            //the entire select query up to the end of the group by (omitting any Top X)
            syntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(static c =>
                c.LocationToInsert is >= QueryComponent.SELECT and < QueryComponent.OrderBy &&
                c.Role != CustomLineRole.TopX))),

            syntaxHelper.Escape(countAlias),
            syntaxHelper.Escape(pivotAlias),
            syntaxHelper.Escape(GetDatePartOfColumn(query.Axis.AxisIncrement,"axis.dt")),
            axisColumnAlias
        );

        return part1 + part2;
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        var syntaxHelper = query.SyntaxHelper;

        var part1 = GetPivotPart1(query, out var pivotAlias, out var countAlias, out _);

        syntaxHelper.SplitLineIntoSelectSQLAndAlias(nonPivotColumn.Text, out var nonPivotColumnSelect, out var nonPivotColumnAlias);

        //ensure we have an alias for the non pivot column
        if (string.IsNullOrWhiteSpace(nonPivotColumnAlias))
            nonPivotColumnAlias = syntaxHelper.GetRuntimeName(nonPivotColumnSelect);

        //The dynamic query in which we assemble a query string and EXECUTE it
        var part2 = string.Format("""

                                  /*DYNAMIC PIVOT*/
                                  declare @Query varchar(MAX)

                                  SET @Query = '
                                  {0}

                                  /*Would normally be Select * but must make it IsNull to ensure we see 0s instead of null*/
                                  select
                                  {1},
                                  '+@FinalSelectList+'
                                  from
                                  (
                                      {2}
                                  ) s
                                  PIVOT
                                  (
                                  	sum({3})
                                  	for {4} in ('+@Columns+') --The dynamic Column list we just fetched at top of query

                                  ) piv
                                  ORDER BY
                                  {1}'

                                  EXECUTE(@Query)

                                  """,
            //anything before the SELECT (i.e. parameters)
            syntaxHelper.Escape(string.Join(Environment.NewLine,
                query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT))),
            syntaxHelper.Escape(nonPivotColumnAlias),

            //the entire select query up to the end of the group by (omitting any Top X)
            syntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(static c =>
                c.LocationToInsert is >= QueryComponent.SELECT and < QueryComponent.OrderBy &&
                c.Role != CustomLineRole.TopX))),

            syntaxHelper.Escape(countAlias),
            syntaxHelper.Escape(pivotAlias));

        return part1 + part2;
    }

    private string GetPivotPart1(AggregateCustomLineCollection query, out string pivotAlias,out string countAlias, out string axisColumnAlias)
    {
        var syntaxHelper = query.SyntaxHelper;

        //find the pivot column e.g. 'hb_extract AS Healthboard'
        var pivotSelectLine = query.PivotSelect;
        var pivotSqlWithoutAlias = pivotSelectLine.GetTextWithoutAlias(syntaxHelper);
        pivotAlias = pivotSelectLine.GetAliasFromText(syntaxHelper);

        //ensure it has an RHS
        if (string.IsNullOrWhiteSpace(pivotAlias))
            pivotAlias = syntaxHelper.GetRuntimeName(pivotSqlWithoutAlias);

        var countSqlWithoutAlias = query.CountSelect.GetTextWithoutAlias(syntaxHelper);
        countAlias = query.CountSelect.GetAliasFromText(syntaxHelper);

        var axisColumnWithoutAlias = query.AxisSelect?.GetTextWithoutAlias(query.SyntaxHelper);
        axisColumnAlias = query.AxisSelect?.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        //if there is an axis we don't want to pivot on values that are outside that axis restriction.
        if(query.Axis != null)
            WrapAxisColumnWithDatePartFunction(query,axisColumnAlias);
        else
        {
            axisColumnAlias = null;
            axisColumnWithoutAlias = null;
        }

        //Part 1 is where we get all the unique values from the pivot column (after applying the WHERE logic)

        var anyFilters = query.Lines.Any(static l => l.LocationToInsert == QueryComponent.WHERE);

        var orderBy = $"{countSqlWithoutAlias} desc";

        if (query.TopXOrderBy != null)
            orderBy = query.TopXOrderBy.Text;

        var havingSqlIfAny = string.Join(Environment.NewLine,
            query.Lines.Where(static l => l.LocationToInsert == QueryComponent.Having).Select(static l => l.Text));

        var part1 = string.Format(
            """

            /*DYNAMICALLY FETCH COLUMN VALUES FOR USE IN PIVOT*/
            DECLARE @Columns as VARCHAR(MAX)
            {0}

            /*Get distinct values of the PIVOT Column if you have columns with values T and F and Z this will produce [T],[F],[Z] and you will end up with a pivot against these values*/
            set @Columns = (
            {1}
             ',' + QUOTENAME({2}) as [text()]
            {3}
            {4}
            {5} ( {2} IS NOT NULL and {2} <> '' {7})
            group by
            {2}
            {8}
            order by
            {6}
            FOR XML PATH(''), root('MyString'),type
            ).value('/MyString[1]','varchar(max)')

            set @Columns = SUBSTRING(@Columns,2,LEN(@Columns))

            DECLARE @FinalSelectList as VARCHAR(MAX)
            SET @FinalSelectList = {9}

            --Split up that pesky string in tsql which has the column names up into array elements again
            DECLARE @value varchar(8000)
            DECLARE @pos INT
            DECLARE @len INT
            set @pos = 0
            set @len = 0

            WHILE CHARINDEX('],', @Columns +',', @pos+1)>0
            BEGIN
                set @len = CHARINDEX('],[', @Columns +'],[', @pos+1) - @pos
                set @value = SUBSTRING(@Columns, @pos+1, @len)
                    
                --We are constructing a version that turns: '[fish],[lama]' into 'ISNULL([fish],0) as [fish], ISNULL([lama],0) as [lama]'
                SET @FinalSelectList = @FinalSelectList + ', ISNULL(' + @value  + ',0) as ' + @value
            
                set @pos = CHARINDEX('],[', @Columns +'],[', @pos+@len) +1
            END

            if LEFT(@FinalSelectList,1)  = ','
            	SET @FinalSelectList = RIGHT(@FinalSelectList,LEN(@FinalSelectList)-1)


            """,
            //select SQL and parameter declarations
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert < QueryComponent.SELECT)),
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert == QueryComponent.SELECT)),
            pivotSqlWithoutAlias,

            //FROM and JOINs that are not to the calendar table
            string.Join(Environment.NewLine,
                query.Lines.Where(static l =>
                    l.LocationToInsert == QueryComponent.FROM || (l.LocationToInsert == QueryComponent.JoinInfoJoin &&
                                                                  l.Role != CustomLineRole.Axis))),
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert == QueryComponent.WHERE)),
            anyFilters ? "AND" : "WHERE",
            orderBy,
            axisColumnWithoutAlias == null ? "": $"AND  {axisColumnWithoutAlias} is not null",
            havingSqlIfAny,
            query.Axis != null ? "'joinDt'":"''"
        );
        return part1;
    }


}