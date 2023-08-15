using System;
using System.Collections.Generic;
using System.Linq;

namespace FAnsi.Discovery.QuerySyntax.Aggregation;

/// <summary>
/// A collection of <see cref="CustomLine"/> which together make a GROUP BY with an optional Calendar table column
/// and optional dynamic pivot column
/// </summary>
public class AggregateCustomLineCollection
{
    public List<CustomLine> Lines { get; set; }
    public IQueryAxis Axis { get; set; }
    public IQuerySyntaxHelper SyntaxHelper { get; }

    public AggregateCustomLineCollection(List<CustomLine> queryLines, IQueryAxis axisIfAny, IQuerySyntaxHelper querySyntaxHelper)
    {
        Lines = queryLines;
        Axis = axisIfAny;
        SyntaxHelper = querySyntaxHelper;

        Validate();
    }

    private void Validate()
    {
        //if we have any axis bits
        if (Axis == null && AxisSelect == null && AxisGroupBy == null) return;
        //we must have all the axis bits
        if(AxisSelect == null || AxisGroupBy is null or null)
            throw new AggregateCustomLineCollectionException(FAnsiStrings.AggregateCustomLineCollection_Validate_AggregateCustomLineCollection_is_missing_some__but_not_all__Axis_components);
    }

    /// <summary>
    /// The single aggregate function line e.g. "count(distinct chi) as Fish,"
    /// </summary>
    public CustomLine CountSelect => Lines.SingleOrDefault(l => l.Role == CustomLineRole.CountFunction && l.LocationToInsert == QueryComponent.QueryTimeColumn);

    /// <summary>
    /// The (optional) single line of SELECT SQL which is the Axis join column e.g. "[MyDb]..[mytbl].[AdmissionDate] as Admt,"
    /// </summary>
    public CustomLine AxisSelect => Lines.SingleOrDefault(l => l.Role == CustomLineRole.Axis && l.LocationToInsert == QueryComponent.QueryTimeColumn);


    /// <summary>
    /// The (optional) single line of GROUP BY SQL which matches exactly the SQL of <see cref="AxisSelect"/>
    /// </summary>
    public CustomLine AxisGroupBy => Lines.SingleOrDefault(l => l.LocationToInsert == QueryComponent.GroupBy && l.Role == CustomLineRole.Axis);

    /// <summary>
    /// The (optional) single line of SELECT SQL which is the dynamic pivot column e.g. "[MyDb]..[mytbl].[Healthboard] as hb,"
    /// </summary>
    public CustomLine PivotSelect => Lines.SingleOrDefault(l => l.Role == CustomLineRole.Pivot && l.LocationToInsert == QueryComponent.QueryTimeColumn);


    /// <summary>
    /// The (optional) single line of ORDER BY SQL which restricts which records are returned when doing a dynamic pivot e.g. only dynamic pivot on the
    /// top 5 drugs ordered by SUM of prescriptions
    /// </summary>
    public CustomLine TopXOrderBy => Lines.SingleOrDefault(l => l.LocationToInsert == QueryComponent.OrderBy && l.Role == CustomLineRole.TopX);

    /// <summary>
    /// Returns all concatenated SQL for all <see cref="Lines"/> between the inclusive boundaries from/to
    /// </summary>
    /// <param name="from">inclusive start section from which to return rows</param>
    /// <param name="to">inclusive end section from which to return rows</param>
    public string GetLines(QueryComponent from, QueryComponent to)
    {
        return string.Join(Environment.NewLine,
            Lines.Where(c => c.LocationToInsert >= from && c.LocationToInsert <= to));
    }
}