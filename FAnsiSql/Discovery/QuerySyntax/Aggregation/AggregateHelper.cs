using System;
using System.Collections.Generic;
using System.Linq;

namespace FAnsi.Discovery.QuerySyntax.Aggregation;

public abstract class AggregateHelper:IAggregateHelper
{
    public string BuildAggregate(List<CustomLine> queryLines, IQueryAxis axisIfAny)
    {
        var lines = new AggregateCustomLineCollection(queryLines, axisIfAny, GetQuerySyntaxHelper());
            
        //no axis no pivot
        if (lines.AxisSelect == null  && lines.PivotSelect == null)
            return BuildBasicAggregate(lines);  

        //axis (no pivot)
        if (lines.PivotSelect == null)
            return BuildAxisAggregate(lines);
            
        //pivot (no axis)
        if (lines.AxisSelect == null)
            return BuildPivotOnlyAggregate(lines,GetPivotOnlyNonPivotColumn(lines));

        //pivot and axis
        return BuildPivotAndAxisAggregate(lines);
    }

    private CustomLine GetPivotOnlyNonPivotColumn(AggregateCustomLineCollection query)
    {
        var nonPivotColumn = query.Lines.Where(l => l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.None).ToArray();
        if(nonPivotColumn.Length != 1)
            throw new Exception("Pivot is only valid when there are 3 SELECT columns, an aggregate (e.g. count(*)), a pivot and a final column");

        return nonPivotColumn[0];
    }

    protected abstract IQuerySyntaxHelper GetQuerySyntaxHelper();

    protected string BuildBasicAggregate(AggregateCustomLineCollection query) => string.Join(Environment.NewLine, query.Lines);

    /// <summary>
    /// Builds an SQL GROUP BY query in from the lines in <paramref name="query"/> where records are counted and put into
    /// buckets according to the interval defined in <see cref="AggregateCustomLineCollection.Axis"/> based on the date SQL
    /// (usually a column name) in <see cref="AggregateCustomLineCollection.AxisSelect"/>
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    protected abstract string BuildAxisAggregate(AggregateCustomLineCollection query);
        
    protected abstract string BuildPivotOnlyAggregate(AggregateCustomLineCollection query,CustomLine nonPivotColumn);

    protected abstract string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query);


    /// <summary>
    /// Changes the axis column in the GROUP BY section of the query (e.g. "[MyDb]..[mytbl].[AdmissionDate],") and
    /// the axis column in the SELECT section of the query (e.g. "[MyDb]..[mytbl].[AdmissionDate] as Admt,")  with
    /// the appropriate axis increment (e.g. "YEAR([MyDb]..[mytbl].[AdmissionDate])," and "YEAR([MyDb]..[mytbl].[AdmissionDate]) as Admt,")
    /// </summary>
    /// <param name="query"></param>
    /// <param name="axisColumnAlias"></param>
    protected void WrapAxisColumnWithDatePartFunction(AggregateCustomLineCollection query, string axisColumnAlias)
    {
        if(string.IsNullOrWhiteSpace(axisColumnAlias))
            throw new ArgumentNullException(nameof(axisColumnAlias));

        var axisGroupBy = query.AxisGroupBy;
        var axisColumnWithoutAlias = query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper);

        var axisColumnEndedWithComma = query.AxisSelect.Text.EndsWith(",");
        query.AxisSelect.Text =
            $"{GetDatePartOfColumn(query.Axis.AxisIncrement, axisColumnWithoutAlias)} AS {axisColumnAlias}{(axisColumnEndedWithComma ? "," : "")}";

        var groupByEndedWithComma = axisGroupBy.Text.EndsWith(",");
        axisGroupBy.Text = GetDatePartOfColumn(query.Axis.AxisIncrement, axisColumnWithoutAlias) + (groupByEndedWithComma ? "," : "");
    }

    public abstract string GetDatePartOfColumn(AxisIncrement increment, string columnSql);
}