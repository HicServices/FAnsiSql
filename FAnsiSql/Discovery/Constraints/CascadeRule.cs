namespace FAnsi.Discovery.Constraints;

/// <summary>
/// Describes the action performed when a DELETE or UPDATE command is executed on a field tied to a foreign key constraint
/// </summary>
public enum CascadeRule
{
    /// <summary>
    /// Action is not known or understood
    /// </summary>
    Unknown,

    /// <summary>
    /// Child rows are deleted when Parent row is deleted
    /// </summary>
    Delete,

    /// <summary>
    /// No action is taken (may result in DELETE command failing when deleting Parent rows)
    /// </summary>
    NoAction,

    /// <summary>
    /// Child rows are set to NULL when Parent row is deleted
    /// </summary>
    SetNull,

    /// <summary>
    /// Child rows are set to the field's default value when Parent row is deleted
    /// </summary>
    SetDefault
}