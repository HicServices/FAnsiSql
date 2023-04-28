namespace FAnsi.Discovery.QuerySyntax.Aggregation;

/// <summary>
/// Describes a date/time axis granularity.
/// </summary>
public enum AxisIncrement
{
    /// <summary>
    /// field should contain values expressed down to the individual day
    /// </summary>
    Day = 1,

    /// <summary>
    /// field should contain values expressed down to the individual month
    /// </summary>
    Month = 2,

    /// <summary>
    /// field should contain values expressed down to the individual year
    /// </summary>
    Year = 3,

    /// <summary>
    /// field should contain values expressed down to the individual quarter
    /// </summary>
    Quarter=4
}